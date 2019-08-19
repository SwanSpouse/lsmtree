/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.indeed.lsmtree.recordlog;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;
import com.indeed.util.compress.BlockDecompressorStream;
import com.indeed.util.compress.CompressionCodec;
import com.indeed.util.compress.Decompressor;
import com.indeed.util.compress.SnappyCodec;
import com.indeed.util.core.Either;
import com.indeed.util.io.BufferedFileDataOutputStream;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.io.RandomAccessDataInput;
import com.indeed.util.io.SyncableDataOutput;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.serialization.Serializer;
import com.indeed.util.io.UnsafeByteArrayOutputStream;
import com.indeed.util.io.VIntUtils;
import com.indeed.util.mmap.DirectMemory;
import com.indeed.util.mmap.HeapMemory;
import com.indeed.util.mmap.MMapBuffer;
import com.indeed.util.mmap.Memory;
import com.indeed.util.mmap.MemoryDataInput;
import fj.data.Option;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import static com.indeed.util.core.Either.Right;

/**
 * @author jplaisance
 */
public final class BlockCompressedRecordFile<E> implements RecordFile<E> {

    private static final Logger log = Logger.getLogger(BlockCompressedRecordFile.class);

    private final String file;
    private final Serializer<E> serializer;
    private final CompressionCodec codec;
    private final int blockSize;
    private final int padBits;

    // TODO ??? 这个还不是很理解
    private final Supplier<? extends Either<IOException, ? extends RandomAccessDataInput>> inputSupplier;

    private final BlockCache blockCache;

    private final int shift;
    private final long mask;
    private final int pad;
    private final long padMask;
    private final int maxChunkSize;

    private final SharedReference<Closeable> closeableRef;

    // 初始化计数器
    private static final AtomicLong openFileCounter = new AtomicLong(0);

    // 打开文件的个数
    public static long getOpenFileCount() {
        return openFileCounter.get();
    }

    // 打开BlockCompressedRecordFile
    public static <E> BlockCompressedRecordFile<E> open(final File file, Serializer<E> serializer, CompressionCodec codec, BlockingQueue<Decompressor> decompressorPool, int blockSize, int recordIndexBits, int padBits, boolean mlockFiles, int maxChunkSize) throws IOException {
        final MMapBuffer buffer = new MMapBuffer(file, FileChannel.MapMode.READ_ONLY, ByteOrder.BIG_ENDIAN);
        try {
            if (mlockFiles) {
                buffer.mlock(0, buffer.memory().length());
            }
            final Memory memory = buffer.memory();
            // 计数器
            openFileCounter.incrementAndGet();
            return new BlockCompressedRecordFile<E>(
                    new Supplier<Either<IOException, MemoryRandomAccessDataInput>>() {
                        public Either<IOException, MemoryRandomAccessDataInput> get() {
                            return Right.of(new MemoryRandomAccessDataInput(memory));
                        }
                    },
                    new Closeable() {
                        public void close() throws IOException {
                            openFileCounter.decrementAndGet();
                            buffer.close();
                        }
                    },
                    file.getAbsolutePath(),
                    serializer,
                    codec,
                    decompressorPool,
                    blockSize,
                    recordIndexBits,
                    padBits,
                    maxChunkSize
            );
        } catch (Throwable t) {
            Closeables2.closeQuietly(buffer, log);
            Throwables.propagateIfInstanceOf(t, IOException.class);
            throw Throwables.propagate(t);
        }
    }

    public static @Nullable
    byte[] getMetadata(File file) throws IOException {
        final long length = file.length();
        final MMapBuffer buffer = new MMapBuffer(file, 0, length, FileChannel.MapMode.READ_ONLY, ByteOrder.BIG_ENDIAN);
        final DirectMemory memory = buffer.memory();
        final int metadataLength = memory.getInt(length - 12);
        if (metadataLength == Integer.MAX_VALUE) return null;
        final byte[] metadata = new byte[metadataLength];
        memory.getBytes(length - 12 - metadataLength, metadata);
        return metadata;
    }

    // 构造函数
    public BlockCompressedRecordFile(final Supplier<? extends Either<IOException, ? extends RandomAccessDataInput>> inputSupplier, final Closeable closeable, String file, Serializer<E> serializer, CompressionCodec codec, BlockingQueue<Decompressor> decompressorPool, int blockSize, int recordIndexBits, int padBits, int maxChunkSize) throws IOException {
        this.inputSupplier = inputSupplier;
        this.file = file;
        this.serializer = serializer;
        this.codec = codec;
        this.blockSize = blockSize;
        this.padBits = padBits;
        this.maxChunkSize = maxChunkSize;
        pad = 1 << padBits;
        padMask = ~(long) (pad - 1);
        shift = Math.max(recordIndexBits - padBits, 0);
        mask = (1L << recordIndexBits) - 1;
        closeableRef = SharedReference.create(closeable);
        try {
            blockCache = new BlockCache(decompressorPool);
        } catch (Throwable t) {
            Closeables2.closeQuietly(closeableRef, log);
            Throwables.propagateIfInstanceOf(t, IOException.class);
            throw Throwables.propagate(t);
        }
    }

    public static final class Writer<E> implements RecordFile.Writer<E> {

        private final SyncableDataOutput out;

        private final int[] lengthBuffer;
        private final UnsafeByteArrayOutputStream currentBlockBytes;
        private final DataOutputStream currentBlockOut;
        private int numRecords = 0;
        private long blockAddress = 0;
        private final Serializer<E> serializer;
        private final CompressionCodec codec;

        private final int blockSize;
        private final int shift;
        private final int pad;

        public static <E> Writer<E> open(File file, Serializer<E> serializer, CompressionCodec codec, int blockSize, int recordIndexBits, int padBits) throws FileNotFoundException {
            final SyncableDataOutput out = new BufferedFileDataOutputStream(file, ByteOrder.BIG_ENDIAN, 16384);
            return new Writer<E>(out, serializer, codec, blockSize, recordIndexBits, padBits);
        }

        public Writer(SyncableDataOutput out, Serializer<E> serializer, CompressionCodec codec, int blockSize, int recordIndexBits, int padBits) {
            // 每块的大小不能超过16MB
            if (blockSize > 1024 * 1024 * 16)
                throw new IllegalArgumentException("block size must be less than 2^24");
            this.out = out;
            lengthBuffer = new int[1 << recordIndexBits];
            currentBlockBytes = new UnsafeByteArrayOutputStream(blockSize);
            currentBlockOut = new DataOutputStream(currentBlockBytes);
            // TODO pad 是用来进行对齐的？
            pad = 1 << padBits;
            shift = Math.max(recordIndexBits - padBits, 0);
            this.serializer = serializer;
            this.codec = codec;
            this.blockSize = blockSize;
        }

        // append 操作
        public long append(final E entry) throws IOException {
            if ((currentBlockBytes.size() >= blockSize && numRecords > 0) || numRecords == lengthBuffer.length) {
                // 刷写到文件中
                flushBuffer();
            }
            // 获取应该写入的位置（当前块的末尾）
            final int start = currentBlockBytes.size();
            // 将数据写入到块的末尾
            serializer.write(entry, currentBlockOut);
            // 计算写入数据的长度
            final int length = (currentBlockBytes.size() - start);
            // 将写入数据的长度记录到lengthBuffer中
            lengthBuffer[numRecords] = length;
            // TODO ???
            final long ret = blockAddress + numRecords;
            // 记录数+1
            numRecords++;
            return ret;
        }

        // 将内存中的数据刷写到磁盘中
        //  compressorStream中的数据格式「numRecord」「recordLength 0」「recordLength 1」... 「recordLength N」「Data」
        private void flushBuffer() throws IOException {
            final UnsafeByteArrayOutputStream compressedBuffer = new UnsafeByteArrayOutputStream(blockSize + 4 * numRecords);
            final CheckedOutputStream checksumStream = new CheckedOutputStream(compressedBuffer, new Adler32());
            final DataOutputStream compressorStream = new DataOutputStream(codec.createOutputStream(checksumStream));
            // 写入记录条数
            compressorStream.writeInt(numRecords);
            for (int i = 0; i < numRecords; i++) {
                // 写入每条记录的大小
                VIntUtils.writeVInt((OutputStream) compressorStream, lengthBuffer[i]);
            }
            // 写入block 的块大小
            compressorStream.write(currentBlockBytes.getByteArray(), 0, currentBlockBytes.size());
            compressorStream.close();

            // 写入 compressedBuffer中的数据长度
            out.writeInt(compressedBuffer.size());
            // 计算校验和
            final int checksum = (int) checksumStream.getChecksum().getValue();
            out.writeInt(checksum);
            out.write(compressedBuffer.getByteArray(), 0, compressedBuffer.size());
            // 清空Block
            currentBlockBytes.reset();
            numRecords = 0;
            final int padLength = (int) (pad - out.position() % pad);
            if (padLength != pad) {
                for (int i = 0; i < padLength; i++) {
                    out.writeByte(0);
                }
            }
            blockAddress = out.position() << shift;
        }

        public void close() throws IOException {
            if (numRecords > 0) {
                flushBuffer();
            }
            out.writeInt(Integer.MAX_VALUE);
            out.writeLong(out.position() + 8);
            out.sync();
            out.close();
        }

        public void close(byte[] metadata) throws IOException {
            if (numRecords > 0) {
                flushBuffer();
            }
            out.writeInt(Integer.MAX_VALUE);
            out.write(metadata);
            out.writeInt(metadata.length);
            out.writeLong(out.position() + 8);
            out.sync();
            out.close();
        }

        @Override
        public void sync() throws IOException {
            if (numRecords > 0) flushBuffer();
            out.sync();
        }

        public static final class Builder<E> {

            private final File file;
            private final Serializer<E> serializer;
            private CompressionCodec codec;
            private int blockSize = 16384;
            private int recordIndexBits = 10;
            private int padBits = 6;

            public Builder(final File file, final Serializer<E> serializer) {
                this.file = file;
                this.serializer = serializer;
            }

            public void setCodec(final CompressionCodec codec) {
                this.codec = codec;
            }

            public void setBlockSize(final int blockSize) {
                this.blockSize = blockSize;
            }

            public void setRecordIndexBits(final int recordIndexBits) {
                this.recordIndexBits = recordIndexBits;
            }

            public void setPadBits(final int padBits) {
                this.padBits = padBits;
            }

            public Writer<E> build() throws IOException {
                if (codec == null) {
                    codec = new SnappyCodec();
                }
                return Writer.open(file, serializer, codec, blockSize, recordIndexBits, padBits);
            }
        }
    }

    @Override
    public E get(long address) throws IOException {
        final long blockAddress = (address >>> shift) & padMask;
        final Option<BlockCacheEntry> blockOption = blockCache.get(blockAddress).get();
        if (blockOption.isNone()) throw new IOException("illegal address " + address + " in file " + file);
        final BlockCacheEntry block = blockOption.some();
        final int recordIndex = (int) (address & mask);
        if (recordIndex >= block.size()) {
            throw new IOException("there are only " + block.size() + " in block at address " + blockAddress + ", seek request is for record number " + recordIndex);
        }
        return serializer.read(new MemoryDataInput(block.get(recordIndex)));
    }

    @Override
    public RecordFile.Reader<E> reader() throws IOException {
        return new Reader(closeableRef.copy());
    }

    @Override
    public RecordFile.Reader<E> reader(long address) throws IOException {
        return new Reader(closeableRef.copy(), address);
    }

    @Override
    public void close() throws IOException {
        closeableRef.close();
    }

    private final class Reader implements RecordFile.Reader<E> {

        private long position;
        // 返回当前的数据
        private E current;

        // 当前记录数
        private int currentRecord = 0;

        // Option是为了防止空指针问题而引入的容器
        private Option<BlockCacheEntry> currentBlock;
        private long blockAddress = 0;

        private boolean done = false;
        private boolean initialized = false;
        private final SharedReference<Closeable> ref;

        public Reader(SharedReference<Closeable> ref) throws IOException {
            this.ref = ref;
        }

        // 根据seekAddress初始化reader
        public Reader(SharedReference<Closeable> ref, long seekAddress) throws IOException {
            this.ref = ref;
            initialized = true;
            // 从seekAddress地址开始获取对应的Block
            final long newBlockAddress = (seekAddress >>> shift) & padMask;
            currentBlock = blockCache.get(newBlockAddress).get();
            blockAddress = newBlockAddress;
            if (currentBlock.isNone()) {
                done = true;
                throw new IOException("address " + seekAddress + " is invalid because block does not exist in file " + file);
            }
            final BlockCacheEntry block = currentBlock.some();
            currentRecord = (int) (seekAddress & mask);
            if (currentRecord >= block.size()) {
                done = true;
                throw new IOException("there are only " + block.size() + " in block at address " + newBlockAddress + ", seek request is for record number " + currentRecord);
            }
        }

        @Override
        public boolean next() throws IOException {
            if (!initialized) {
                currentBlock = blockCache.get(0L).get();
                if (currentBlock.isNone()) done = true;
                initialized = true;
            }
            if (done) {
                return false;
            }
            BlockCacheEntry block = currentBlock.some();
            // 如果当前记录是当前Block的最后一条记录，则从下一个Block中来找
            if (currentRecord == block.size()) {
                blockAddress = block.getNextBlockStartAddress();
                currentBlock = blockCache.get(blockAddress).get();
                currentRecord = 0;
                if (currentBlock.isNone()) {
                    done = true;
                    return false;
                }
                block = currentBlock.some();
            }
            position = (blockAddress << shift) + currentRecord;
            current = serializer.read(new MemoryDataInput(block.get(currentRecord)));
            currentRecord++;
            return true;
        }

        @Override
        public long getPosition() {
            return position;
        }

        @Override
        public E get() {
            return current;
        }

        @Override
        public void close() throws IOException {
            ref.close();
        }
    }

    private final class BlockCache {

        private final LoadingCache<Long, Either<IOException, Option<BlockCacheEntry>>> cache;

        private final BlockingQueue<Decompressor> decompressorPool;

        public BlockCache(BlockingQueue<Decompressor> decompressorPool)
                throws IOException {
            this.decompressorPool = decompressorPool;
            cache = CacheBuilder.newBuilder().weakValues().build(readBlock);
        }

        private final CacheLoader<Long, Either<IOException, Option<BlockCacheEntry>>> readBlock = new CacheLoader<Long, Either<IOException, Option<BlockCacheEntry>>>() {
            @Override
            public Either<IOException, Option<BlockCacheEntry>> load(final Long blockAddress) {
                final Either<IOException, ? extends RandomAccessDataInput> input = inputSupplier.get();
                RandomAccessDataInput in = null;
                try {
                    in = input.get();
                    in.seek(blockAddress);
                    // 首先读取block的长度
                    final int blockLength = in.readInt();
                    if (blockLength == Integer.MAX_VALUE)
                        return Either.Right.of(Option.<BlockCacheEntry>none());
                    if (blockLength < 4)
                        throw new IOException("block length for block at address " + blockAddress + " in file " + file + " is " + blockLength + " which is less than 4. this is not possible. this address is probably no good.");
                    // 计算得到blockEnd的地址
                    final long blockEnd = (((blockAddress + 8 + blockLength - 1) >>> padBits) + 1) << padBits;
                    final long maxAddress = in.length() - 12;
                    if (blockLength < 0 || blockEnd > maxAddress)
                        throw new IOException("block address " + blockAddress + " in file " + file + " is no good, length is " + blockLength + " and end of data is " + maxAddress);
                    // 读取校验和
                    final int checksum = in.readInt();
                    if (maxChunkSize > 0) {
                        final int chunkLength = in.readInt();
                        if (chunkLength > maxChunkSize)
                            throw new IOException("first chunk length (" + chunkLength + ") for block at address " + blockAddress + " in file " + file + " is greater than " + maxChunkSize + ". while this may be correct it is extremely unlikely and this is probably a bad address.");
                        // 在这里跳过长度字节和校验和
                        in.seek(blockAddress + 8);
                    }
                    // 读取数据
                    final byte[] compressedBytes = new byte[blockLength];
                    in.readFully(compressedBytes);
                    final int padLength = (int) (pad - in.position() % pad);
                    if (padLength != pad) {
                        in.seek(in.position() + padLength);
                    }
                    // 进行解压缩
                    final CheckedInputStream checksumStream = new CheckedInputStream(new ByteArrayInputStream(compressedBytes), new Adler32());
                    Decompressor decompressor = decompressorPool.poll();
                    if (decompressor == null) {
                        decompressor = codec.createDecompressor();
                    }
                    decompressor.reset();
                    final InputStream decompressed = new BlockDecompressorStream(checksumStream, decompressor, blockSize * 2);
                    final ByteArrayOutputStream decompressedByteStream = new ByteArrayOutputStream(blockSize * 2);
                    ByteStreams.copy(decompressed, decompressedByteStream);
                    decompressed.close();
                    decompressedByteStream.close();
                    decompressorPool.offer(decompressor);
                    // 校验校验和
                    if (((int) checksumStream.getChecksum().getValue()) != checksum)
                        throw new IOException("checksum for chunk at block address " + blockAddress + " does not match data");
                    final byte[] decompressedBytes = decompressedByteStream.toByteArray();
                    final CountingInputStream counter = new CountingInputStream(new ByteArrayInputStream(decompressedBytes));
                    final DataInputStream dataInput = new DataInputStream(counter);
                    // 首先读取记录条数
                    final int numRecords = dataInput.readInt();
                    // 初始化各个块的大小，
                    final int[] recordOffsets = new int[numRecords + 1];
                    int sum = 0;
                    for (int i = 0; i < numRecords; i++) {
                        recordOffsets[i] = sum;
                        final int delta = VIntUtils.readVInt((InputStream) dataInput);
                        sum += delta;
                    }
                    recordOffsets[numRecords] = sum;
                    final int count = (int) counter.getCount();
                    dataInput.close();
                    // 读取真正的数据
                    final byte[] block = new byte[decompressedBytes.length - count];
                    System.arraycopy(decompressedBytes, count, block, 0, block.length);
                    // 在这里把数据吐回去
                    return Either.Right.of(Option.some(new BlockCacheEntry(recordOffsets, new HeapMemory(block, ByteOrder.BIG_ENDIAN), in.position())));
                } catch (IOException e) {
                    log.info("error reading block at address " + blockAddress + " in file " + file, e);
                    return Either.Left.of(e);
                } finally {
                    Closeables2.closeQuietly(in, log);
                }
            }
        };

        public Either<IOException, Option<BlockCacheEntry>> get(final Long key) {
            return cache.getUnchecked(key);
        }
    }

    // BlockCacheEntry Block中的一条记录
    private static final class BlockCacheEntry {
        private final int[] recordOffsets;
        private final Memory block;
        private final long nextBlockStartAddress;

        // 根据 recordOffsets来构造BlockCacheEntry
        public BlockCacheEntry(final int[] recordOffsets, final Memory block, final long nextBlockStartAddress) {
            this.recordOffsets = recordOffsets;
            this.block = block;
            this.nextBlockStartAddress = nextBlockStartAddress;
        }

        // TODO ??? 这个offset数组里面还放了其他别的东西吗？
        public int size() {
            return recordOffsets.length - 1;
        }

        public Memory get(int index) {
            // 首先要获取到对应数据记录的长度
            final int length = recordOffsets[index + 1] - recordOffsets[index];
            // 从Block中返回length长度的数据
            return block.slice(recordOffsets[index], length);
        }

        public long getNextBlockStartAddress() {
            return nextBlockStartAddress;
        }
    }

    // 构造器，用来构造Block
    public final static class Builder<E> {

        private File file;

        private Serializer<E> serializer;

        private CompressionCodec codec;

        private BlockingQueue<Decompressor> decompressorPool = null;

        private int blockSize = 16384;

        private int recordIndexBits = 10;

        private int padBits = 6;

        private boolean mlockFiles = false;

        private int maxChunkSize = 128 * 1024 * 1024;

        public Builder(
                final File file,
                final Serializer<E> serializer,
                final CompressionCodec codec
        ) {
            this.file = file;
            this.serializer = serializer;
            this.codec = codec;
        }

        public Builder<E> setFile(final File file) {
            this.file = file;
            return this;
        }

        public Builder<E> setSerializer(final Serializer<E> serializer) {
            this.serializer = serializer;
            return this;
        }

        public Builder<E> setCodec(final CompressionCodec codec) {
            this.codec = codec;
            return this;
        }

        public Builder<E> setDecompressorPool(final BlockingQueue<Decompressor> decompressorPool) {
            this.decompressorPool = decompressorPool;
            return this;
        }

        public Builder<E> setBlockSize(final int blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        public Builder<E> setRecordIndexBits(final int recordIndexBits) {
            this.recordIndexBits = recordIndexBits;
            return this;
        }

        public Builder<E> setPadBits(final int padBits) {
            this.padBits = padBits;
            return this;
        }

        public Builder<E> setMlockFiles(final boolean mlockFiles) {
            this.mlockFiles = mlockFiles;
            return this;
        }

        public Builder<E> setMaxChunkSize(final int maxChunkSize) {
            this.maxChunkSize = maxChunkSize;
            return this;
        }

        public BlockCompressedRecordFile<E> build() throws IOException {
            decompressorPool = decompressorPool == null ? new LinkedBlockingQueue<Decompressor>() : decompressorPool;
            return open(
                    file,
                    serializer,
                    codec,
                    decompressorPool,
                    blockSize,
                    recordIndexBits,
                    padBits,
                    mlockFiles,
                    maxChunkSize
            );
        }
    }
}

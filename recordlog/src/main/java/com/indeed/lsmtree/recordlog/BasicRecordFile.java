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

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.indeed.util.io.BufferedFileDataOutputStream;
import com.indeed.util.io.UnsafeByteArrayOutputStream;
import com.indeed.util.serialization.Serializer;
import com.indeed.util.mmap.MMapBuffer;
import com.indeed.util.mmap.Memory;
import com.indeed.util.mmap.MemoryDataInput;
import fj.data.Option;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

/**
 * @author jplaisance
 */
public final class BasicRecordFile<E> implements RecordFile<E> {

    private static final byte[] CRC_SEED = Ints.toByteArray(0xC2A3066E);

    private static final Logger log = Logger.getLogger(BasicRecordFile.class);

    final MMapBuffer buffer;

    final Memory memory;

    private final File file;
    private final Serializer<E> serializer;


    public BasicRecordFile(File file, Serializer<E> serializer) throws IOException {
        this.file = file;
        this.serializer = serializer;
        buffer = new MMapBuffer(file, FileChannel.MapMode.READ_ONLY, ByteOrder.BIG_ENDIAN);
        memory = buffer.memory();
    }

    @Override
    public void close() throws IOException {
        buffer.close();
    }

    @Override
    public E get(long address) throws IOException {
        Option<E> option = readAndCheck(address, null);
        if (option.isNone())
            throw new IOException("there is not a valid record at address " + address + " in file " + file.getAbsolutePath());
        return option.some();
    }

    @Override
    public RecordFile.Reader<E> reader() throws IOException {
        return new Reader();
    }

    @Override
    public RecordFile.Reader<E> reader(long address) throws IOException {
        return new Reader(address);
    }

    private Option<E> readAndCheck(long address, MutableLong nextElementStart) throws IOException {
        // [0,1,2,3] 4个byte是总长度
        if (address + 4 > memory.length()) {
            throw new ConsistencyException("not enough bytes in file");
        }
        final int length = memory.getInt(address);
        if (length < 0) {
            return Option.none();
        }
        // [5,6,7,8] 4个byte是checksum
        if (address + 8 > memory.length()) {
            throw new ConsistencyException("not enough bytes in file");
        }
        // 查看数据是否超出了范围
        if (address + 8 + length > memory.length()) {
            throw new ConsistencyException("not enough bytes in file");
        }
        // 获取checksum
        final int checksum = memory.getInt(address + 4);

        MemoryDataInput in = new MemoryDataInput(memory);
        in.seek(address + 8);
        CRC32 crc32 = new CRC32();
        crc32.update(CRC_SEED);
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        // 计算并验证checksum
        crc32.update(bytes);
        if ((int) crc32.getValue() != checksum) {
            throw new ConsistencyException("checksum for record does not match: expected " + checksum + " actual " + (int) crc32.getValue());
        }

        // TODO 最后这里在搞啥，没看懂
        E ret = serializer.read(ByteStreams.newDataInput(bytes));
        if (nextElementStart != null) nextElementStart.setValue(address + 8 + length);
        return Option.some(ret);
    }

    private final class Reader implements RecordFile.Reader<E> {
        MutableLong position;  // 当前位置
        E e; // 当前位置的数据

        boolean done = false; // 标记当前文件是否读完

        private Reader() {
            this(0);
        }

        private Reader(long address) {
            position = new MutableLong(address);
        }

        @Override
        public boolean next() throws IOException {
            try {
                // 从当前位置往后读
                Option<E> option = readAndCheck(position.longValue(), position);
                if (option.isNone()) {
                    done = true;
                    return false;
                }
                e = option.some();
            } catch (ConsistencyException e) {
                done = true;
                log.warn("reading next record in " + file.getAbsolutePath() + " failed with exception", e);
                return false;
            }
            return true;
        }

        @Override
        public long getPosition() {
            return position.longValue();
        }

        @Override
        public E get() {
            return e;
        }

        @Override
        public void close() throws IOException {
        }
    }

    public static final class Writer<E> implements RecordFile.Writer<E> {

        final BufferedFileDataOutputStream out;
        private final Serializer<E> serializer;

        public Writer(File file, Serializer<E> serializer) throws FileNotFoundException {
            this.serializer = serializer;
            out = new BufferedFileDataOutputStream(file, ByteOrder.BIG_ENDIAN, 65536);
        }

        // 具体的写文件实现，会添加一些校验和
        @Override
        public long append(final E entry) throws IOException {
            UnsafeByteArrayOutputStream bytes = new UnsafeByteArrayOutputStream();
            // 首先进行序列化
            serializer.write(entry, new DataOutputStream(bytes));
            // 获取写的位置
            final long start = out.position();
            // 首先写入数据长度
            out.writeInt(bytes.size());
            // 获取checksum
            final CRC32 checksum = new CRC32();
            checksum.update(CRC_SEED);
            checksum.update(bytes.getByteArray(), 0, bytes.size());
            // 写入checksum
            out.writeInt((int) checksum.getValue());
            // 写入data
            out.write(bytes.getByteArray(), 0, bytes.size());
            // 同时返回写入的位置
            return start;
        }

        @Override
        public void close() throws IOException {
            // 写入结尾
            out.writeInt(-1);
            // flush 文件
            out.sync();
            out.close();
        }

        public void sync() throws IOException {
            out.sync();
        }
    }
}

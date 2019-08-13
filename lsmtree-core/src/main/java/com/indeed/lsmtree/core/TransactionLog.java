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
package com.indeed.lsmtree.core;

import com.google.common.io.Closer;
import com.indeed.util.serialization.Serializer;
import com.indeed.lsmtree.recordlog.BasicRecordFile;
import com.indeed.lsmtree.recordlog.RecordFile;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

/**
 * @author jplaisance
 */
public final class TransactionLog {

    private static final Logger log = Logger.getLogger(TransactionLog.class);

    public static class Reader<K, V> implements Closeable {

        private final BasicRecordFile<OpKeyValue<K, V>> recordFile;
        private final RecordFile.Reader<OpKeyValue<K, V>> reader;
        private boolean done; // 用于标识是否读完
        // 以下3个字段用于标识当前的Entry
        private TransactionType type;
        private K key;
        private V value;

        public Reader(File path, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
            // 创建一个reader
            recordFile = new BasicRecordFile<OpKeyValue<K, V>>(path, new OpKeyValueSerialzer<K, V>(keySerializer, valueSerializer));
            reader = recordFile.reader();
        }

        public boolean next() throws IOException {
            if (done) return false;
            try {
                if (!reader.next()) {
                    done = true;
                    return false;
                }
            } catch (IOException e) {
                log.warn("error reading log file, halting log replay at this point", e);
                done = true;
                return false;
            }
            // 读取数据
            final OpKeyValue<K, V> opKeyValue = reader.get();
            type = opKeyValue.type;
            key = opKeyValue.key;
            // 如果这个Type是PUT的话，value不是停留在上一个的值上面了吗？如果是DELETE这里应该给null吧。
            if (type == TransactionType.PUT) {
                value = opKeyValue.value;
            }
            return true;
        }

        public TransactionType getType() {
            return type;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        @Override
        public void close() throws IOException {
            // 现在close文件都是这种操作了吗？
            final Closer closer = Closer.create();
            closer.register(reader);
            closer.register(recordFile);
            closer.close();
        }
    }

    public static class Writer<K, V> implements Closeable {

        private final BasicRecordFile.Writer<OpKeyValue<K, V>> writer;

        private boolean sync;
        private boolean isClosed = false;
        private long sizeInBytes = 0;

        public Writer(File file, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
            this(file, keySerializer, valueSerializer, true);
        }

        public Writer(File file, Serializer<K> keySerializer, Serializer<V> valueSerializer, boolean sync) throws IOException {
            this.sync = sync;
            writer = new BasicRecordFile.Writer(file, new OpKeyValueSerialzer(keySerializer, valueSerializer));
        }

        // 将key value写入到文件中
        public synchronized void put(K key, V value) throws IOException, LogClosedException {
            if (isClosed) {
                throw new LogClosedException();
            }
            try {
                // 先写文件，然后进行同步，写log的时候是直接append就好了。
                sizeInBytes = writer.append(new OpKeyValue<K, V>(TransactionType.PUT, key, value));
                if (sync) {
                    writer.sync();
                }
            } catch (IOException e) {
                close();
                throw e;
            }
        }

        // 将Key从文件中删除，其实也不是删除，就是再添加一个
        public synchronized void delete(K key) throws IOException, LogClosedException {
            if (isClosed) {
                throw new LogClosedException();
            }
            try {
                sizeInBytes = writer.append(new OpKeyValue<K, V>(TransactionType.DELETE, key, null));
                if (sync) {
                    writer.sync();
                }
            } catch (IOException e) {
                close();
                throw e;
            }
        }

        /**
         * Try to clean up all this crap, if any of it throws an exception then rethrow one of them at the end.
         *
         * @throws IOException
         */
        public synchronized void close() throws IOException {
            if (!isClosed) {
                IOException exception = null;
                try {
                    writer.sync();
                } catch (IOException e) {
                    exception = e;
                }
                try {
                    writer.close();
                } catch (IOException e) {
                    exception = e;
                }
                isClosed = true;
                if (exception != null) {
                    throw exception;
                }
            }
        }

        public synchronized long sizeInBytes() throws IOException {
            return sizeInBytes;
        }

        public synchronized void sync() throws IOException {
            try {
                if (!isClosed) writer.sync();
            } catch (IOException e) {
                close();
                throw e;
            }
        }
    }

    // 枚举类型，用来标识TransactionType
    public static enum TransactionType {
        PUT(1),
        DELETE(2);

        int transactionTypeId;

        public int getTransactionTypeId() {
            return transactionTypeId;
        }

        TransactionType(final int transactionTypeId) {
            this.transactionTypeId = transactionTypeId;
        }

        public static TransactionType getTransactionType(int transactionTypeId) {
            switch (transactionTypeId) {
                case 1:
                    return PUT;
                case 2:
                    return DELETE;
                default:
                    throw new IllegalArgumentException(transactionTypeId + " is not a valid transactionTypeId");
            }
        }
    }

    // 基本的Key Value 结构，用TransactionType来标识是否删除
    private static final class OpKeyValue<K, V> {
        TransactionType type;
        K key;
        V value;

        private OpKeyValue(TransactionType type, K key, @Nullable V value) {
            this.type = type;
            this.key = key;
            this.value = value;
        }
    }

    private static final class OpKeyValueSerialzer<K, V> implements Serializer<OpKeyValue<K, V>> {

        Serializer<K> keySerializer;
        Serializer<V> valueSerializer;

        private OpKeyValueSerialzer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        @Override
        public void write(OpKeyValue<K, V> kvOpKeyValue, DataOutput out) throws IOException {
            // 先写入这个数据是PUT还是DELETE
            out.writeByte(kvOpKeyValue.type.getTransactionTypeId());
            // 再写入KEY
            keySerializer.write(kvOpKeyValue.key, out);
            // 如果是PUT的话，还要再写入VALUE，
            if (kvOpKeyValue.type == TransactionType.PUT) {
                valueSerializer.write(kvOpKeyValue.value, out);
            }
        }

        @Override
        public OpKeyValue<K, V> read(DataInput in) throws IOException {
            // 首先读入Type
            final TransactionType type = TransactionType.getTransactionType(in.readByte());
            // 读取Key
            final K key = keySerializer.read(in);
            V value = null;
            // 如果是PUT再读取VALUE
            if (type == TransactionType.PUT) {
                value = valueSerializer.read(in);
            }
            return new OpKeyValue<K, V>(type, key, value);
        }
    }

    public static class LogClosedException extends Exception {
    }
}

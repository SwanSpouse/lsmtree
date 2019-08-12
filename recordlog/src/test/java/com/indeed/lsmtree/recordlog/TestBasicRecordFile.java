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

import com.indeed.util.serialization.array.ByteArraySerializer;
import junit.framework.TestCase;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * @author jplaisance
 */
public final class TestBasicRecordFile extends TestCase {

    private static final Logger log = Logger.getLogger(TestBasicRecordFile.class);

    public void testNormalOperation() throws IOException {
        // 创建文件
        File temp = File.createTempFile("basicrecordfile", ".rec");
        // 写文件
        long[] addresses = writeFile(temp, true);
        // 读文件
        readFile(temp, addresses);
    }

    private void readFile(final File temp, final long[] addresses) throws IOException {
        // 由于随机数的种子是一样的，所以应该产生read和write的时候产生的随机数应该是一样的序列？
        Random rand = new Random(0);
        BasicRecordFile<byte[]> recordFile = new BasicRecordFile<byte[]>(temp, new ByteArraySerializer());
        RecordFile.Reader<byte[]> reader = recordFile.reader();
        // 先通过迭代器进行遍历
        int i = 0;
        try {
            for (i = 0; i < 100000; i++) {
                byte[] bytes = new byte[rand.nextInt(100)];
                for (int j = 0; j < bytes.length; j++) {
                    bytes[j] = (byte) rand.nextInt();
                }
                assertTrue("read failed on iteration " + i, reader.next());
                // 判断数据是否相等
                assertTrue(Arrays.equals(reader.get(), bytes));
            }
            // 最后验证数据个数
            assertFalse(reader.next());
        } catch (IOException e) {
            log.error("read failed on iteration " + i, e);
            throw e;
        }
        // 在这里通过指定地址的方式来进行验证。
        rand = new Random(0);
        try {
            for (i = 0; i < 100000; i++) {
                byte[] bytes = new byte[rand.nextInt(100)];
                for (int j = 0; j < bytes.length; j++) {
                    bytes[j] = (byte) rand.nextInt();
                }
                assertTrue(Arrays.equals(recordFile.get(addresses[i]), bytes));
            }
        } catch (IOException e) {
            log.error("read failed on iteration " + i, e);
            throw e;
        }
        reader.close();
        recordFile.close();
    }

    // 产生随机数据，写入文件，并返回各个数据的起始地址
    private long[] writeFile(final File temp, boolean close) throws IOException {
        Random rand = new Random(0);
        BasicRecordFile.Writer<byte[]> writer = new BasicRecordFile.Writer<byte[]>(temp, new ByteArraySerializer());
        long[] addresses = new long[100000];
        // 生成这么多个随机数
        for (int i = 0; i < 100000; i++) {
            byte[] bytes = new byte[rand.nextInt(100)];
            for (int j = 0; j < bytes.length; j++) {
                bytes[j] = (byte) rand.nextInt();
            }
            // 写入数据同时记录各个数据的地址
            addresses[i] = writer.append(bytes);
        }
        // 将数据写入到文件中，并flush
        if (close) writer.close();
        else writer.sync();
        return addresses;
    }

    public void testNotClosedWriter() throws IOException, InterruptedException {
        File temp = File.createTempFile("basicrecordfile", ".rec");
        long[] addresses = writeFile(temp, false);
        readFile(temp, addresses);
        Thread.sleep(1000);
    }
}

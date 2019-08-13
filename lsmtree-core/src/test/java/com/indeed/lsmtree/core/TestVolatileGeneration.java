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

import com.indeed.util.serialization.IntSerializer;
import com.indeed.util.serialization.LongSerializer;
import junit.framework.TestCase;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

/**
 * @author jplaisance
 */
public final class TestVolatileGeneration extends TestCase {

    private static final Logger log = Logger.getLogger(TestVolatileGeneration.class);

    File tmpDir;

    @Override
    public void setUp() throws Exception {
        tmpDir = File.createTempFile("tmp", "", new File("."));
        tmpDir.delete();
        tmpDir.mkdirs();
    }

    @Override
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(tmpDir);
    }

    public void testIterator() throws Exception {
        final File logPath = new File(tmpDir, "tmp.log");
        VolatileGeneration<Integer, Long> volatileGeneration = new VolatileGeneration(logPath, new IntSerializer(), new LongSerializer(), new ComparableComparator());
        int[] random = new int[1000000];
        Random r = new Random(0);
        for (int i = 0; i < random.length; i++) {
            random[i] = r.nextInt();
        }
        // 产生随机数然添加到volatileGeneration
        for (int element : random) {
            volatileGeneration.put(element, (long) element);
        }
        int[] sorted = new int[random.length];
        System.arraycopy(random, 0, sorted, 0, random.length);
        Arrays.sort(sorted);
        // 验证顺序
        verifyIterationOrder(volatileGeneration, sorted);

        volatileGeneration.close();
        volatileGeneration = new VolatileGeneration<Integer, Long>(new File(tmpDir, "tmp2.log"), new IntSerializer(), new LongSerializer(), new ComparableComparator());
        // 从文件中重新加载数据，并且再次验证数据是否正确
        volatileGeneration.replayTransactionLog(logPath);
        verifyIterationOrder(volatileGeneration, sorted);
    }

    private void verifyIterationOrder(final VolatileGeneration<Integer, Long> volatileGeneration, final int[] sorted) throws IOException {
        // 在这里只遍历了内存中的数据有序。并没有对磁盘上的数据进行验证。
        Iterator<Generation.Entry<Integer, Long>> iterator = volatileGeneration.iterator();
        for (int i = 0; i < sorted.length; i++) {
            while (i + 1 < sorted.length && sorted[i] == sorted[i + 1]) i++;
            assertTrue(iterator.hasNext());
            Generation.Entry<Integer, Long> next = iterator.next();
            assertTrue(next.getKey() == sorted[i]);
            assertTrue(next.getValue() == sorted[i]);
            assertFalse(next.isDeleted());
        }
        assertFalse(iterator.hasNext());
    }
}

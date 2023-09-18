/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.tieredstore.file;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.TieredStoreTestUtil;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Threads(1)
@Fork(1)
@State(Scope.Benchmark)
public class OriginIndexServiceBenchMark {
    private final String storePath = "aaaa";
    private MessageQueue mq;
    private TieredMessageStoreConfig storeConfig;

    TieredIndexFile indexFile;

    @Setup
    public void setUp() throws ClassNotFoundException, NoSuchMethodException, IOException {
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setBrokerName("IndexFileBroker");
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.provider.posix.PosixFileSegment");
        storeConfig.setTieredStoreIndexFileMaxHashSlotNum(TieredIndexServiceStarterTest.slotNum);
        storeConfig.setTieredStoreIndexFileMaxIndexNum(TieredIndexServiceStarterTest.indexNum);
        mq = new MessageQueue("IndexFileTest", storeConfig.getBrokerName(), 1);
        TieredStoreUtil.getMetadataStore(storeConfig);
        TieredStoreExecutor.init();
        TieredFileAllocator fileQueueFactory = new TieredFileAllocator(storeConfig);
        indexFile = new TieredIndexFile(fileQueueFactory, storePath);
    }

    @TearDown
    public void tearDown() throws IOException {
        TieredStoreTestUtil.destroyMetadataStore();
        TieredStoreTestUtil.destroyTempDir(storePath);
        TieredStoreExecutor.shutdown();
        indexFile.destroy();
    }

    @Benchmark
    public AppendResult putKey() {
        int keyNum = (int) (System.currentTimeMillis() % 20000000);
        AppendResult mykey = indexFile.append(mq, 22, "TieredIndexService" + keyNum, 22, 3, System.currentTimeMillis());
        return mykey;
    }

}

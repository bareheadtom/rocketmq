package org.apache.rocketmq.tieredstore.file;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.TieredStoreTestUtil;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.junit.After;
import org.junit.Before;
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

@BenchmarkMode(Mode.AverageTime) // 测试完成时间
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS) // 预热 1 轮，每次 1s
@Measurement(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS) // 测试 5 轮，每次 3s
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
        storeConfig.setTieredStoreIndexFileMaxHashSlotNum(5000000);
        storeConfig.setTieredStoreIndexFileMaxIndexNum(20000000);
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
    public void putKey() {
        int keyNum = (int) (System.currentTimeMillis()%20000000);
        AppendResult mykey = indexFile.append(mq, 22, "TieredIndexService"+keyNum, 22, 3, System.currentTimeMillis());
    }


}

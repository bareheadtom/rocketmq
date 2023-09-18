package org.apache.rocketmq.tieredstore.file;

import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.TieredStoreTestUtil;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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
public class NewIndexServiceBenchMark {
    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);
    private TieredMessageStoreConfig storeConfig;
    private final String storePath = "aaaa";
    private MessageQueue mq;
    private TieredIndexService tieredIndexService;
    TieredFileAllocator tieredFileAllocator;

    @Setup(Level.Iteration)
    public void setUp() {
        try {
            storeConfig = new TieredMessageStoreConfig();
            storeConfig.setBrokerName("IndexFileBroker");
            storeConfig.setStorePathRootDir(storePath);
            storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.provider.posix.PosixFileSegment");
            storeConfig.setTieredStoreIndexFileMaxHashSlotNum(StarterTest.slotNum);
            storeConfig.setTieredStoreIndexFileMaxIndexNum(StarterTest.indexNum);
            mq = new MessageQueue("IndexFileTest", storeConfig.getBrokerName(), 1);
            TieredStoreUtil.getMetadataStore(storeConfig);
            TieredStoreExecutor.init();
            tieredFileAllocator = new TieredFileAllocator(storeConfig);

            tieredIndexService = new TieredIndexService(tieredFileAllocator, storePath);
        } catch (Exception e) {
            logger.error("construct Error");
        }
    }

    @TearDown
    public void tearDowen() {
        this.tieredIndexService.destroy();
        TieredStoreTestUtil.destroyMetadataStore();
        TieredStoreTestUtil.destroyTempDir(storePath);
        TieredStoreExecutor.shutdown();
    }

    @Benchmark
    public AppendResult putKey() {
        int keyNum = (int) (System.currentTimeMillis() % 20000000);
        AppendResult mykey = tieredIndexService.putKey(mq, 22, "TieredIndexService" + keyNum, 22, 3, System.currentTimeMillis());
        return mykey;
    }
}

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
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.TieredStoreTestUtil;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TieredIndexServiceTest {

    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private TieredMessageStoreConfig storeConfig;
    private final String storePath = "aaaa";
    private MessageQueue mq;

    TieredIndexService tieredIndexService;
    TieredFileAllocator tieredFileAllocator;

    @Before
    public void setUp() throws ClassNotFoundException, NoSuchMethodException, IOException {
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setBrokerName("IndexFileBroker");
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.provider.posix.PosixFileSegment");
        storeConfig.setTieredStoreIndexFileMaxHashSlotNum(1);
        storeConfig.setTieredStoreIndexFileMaxIndexNum(4);
        mq = new MessageQueue("IndexFileTest", storeConfig.getBrokerName(), 1);
        TieredStoreUtil.getMetadataStore(storeConfig);
        TieredStoreExecutor.init();
        this.tieredFileAllocator = new TieredFileAllocator(storeConfig);

        tieredIndexService = new TieredIndexService(tieredFileAllocator, storePath);

    }

    @After
    public void tearDown() {
        this.tieredIndexService.destroy();
        TieredStoreTestUtil.destroyMetadataStore();
        TieredStoreTestUtil.destroyTempDir(storePath);
        TieredStoreExecutor.shutdown();
    }

    @Test
    public void putKey() throws ExecutionException, InterruptedException {
        AppendResult mykey = tieredIndexService.putKey(mq, 22, "mykey", 22, 3, 1000);
        Assert.assertEquals(AppendResult.SUCCESS, mykey);
        ConcurrentSkipListMap<Long, IndexFileAccess> concurrentSkipListMap = tieredIndexService.getConcurrentSkipListMap();
        IndexFileAccess indexFileAccess = tieredIndexService.getOrCreateWritingIndexFileAccess(1000);
        Assert.assertEquals(IndexStatusEnum.WRITING, indexFileAccess.getIndexStatus());

        List<IndexItem> queryItems = tieredIndexService.queryAsync(mq.getTopic(), "mykey", 0, System.currentTimeMillis()).get();
        for (IndexItem it : queryItems) {
            logger.info("hashCode:{},topicId:{},queueId:{}, offset:{},size:{}, timeDiff:{}", it.getHashCode(), it.getTopicId(), it.getQueueId(), it.getOffset(), it.getSize(), it.getTimeDiff());
        }
        tieredIndexService.logTieredIndexService();
    }

    @Test
    public void putBatchKeys() throws ExecutionException, InterruptedException {
        String[] keys = {"aa", "bb", "cc"};
        AppendResult ok = tieredIndexService.putBatchKeys(mq, 22, keys, 34, 5, 2000);
        Assert.assertEquals(AppendResult.SUCCESS, ok);
        List<IndexItem> queryItems = tieredIndexService.queryAsync(mq.getTopic(), "aa", 0, System.currentTimeMillis()).get();
        for (IndexItem it : queryItems) {
            logger.info("hashCode:{},topicId:{},queueId:{}, offset:{},size:{}, timeDiff:{}", it.getHashCode(), it.getTopicId(), it.getQueueId(), it.getOffset(), it.getSize(), it.getTimeDiff());
        }
    }

    @Test
    public void testPutFullRolling() {
        AppendResult mykey = tieredIndexService.putKey(mq, 22, "mykey", 22, 3, 1000);
        Assert.assertEquals(AppendResult.SUCCESS, mykey);
        AppendResult mykey2 = tieredIndexService.putKey(mq, 22, "mykey", 22, 3, 2000);
        Assert.assertEquals(AppendResult.SUCCESS, mykey2);
        AppendResult mykey3 = tieredIndexService.putKey(mq, 22, "mykey", 22, 3, 3000);
        Assert.assertEquals(AppendResult.SUCCESS, mykey3);
        AppendResult mykey4 = tieredIndexService.putKey(mq, 22, "mykey", 22, 3, 4000);
        Assert.assertEquals(AppendResult.SUCCESS, mykey4);
        Assert.assertEquals(2, tieredIndexService.getConcurrentSkipListMap().size());
    }

    @Test
    public void doScheduleCompactAndUploadTask() throws IOException {
        AppendResult mykey = tieredIndexService.putKey(mq, 22, "mykey", 22, 1, 1000);
        Assert.assertEquals(AppendResult.SUCCESS, mykey);
        AppendResult mykey2 = tieredIndexService.putKey(mq, 22, "mykey", 22, 2, 2000);
        Assert.assertEquals(AppendResult.SUCCESS, mykey2);
        AppendResult mykey3 = tieredIndexService.putKey(mq, 22, "mykey", 22, 3, 3000);
        Assert.assertEquals(AppendResult.SUCCESS, mykey3);
        AppendResult mykey4 = tieredIndexService.putKey(mq, 22, "mykey", 22, 4, 4000);
        Assert.assertEquals(AppendResult.SUCCESS, mykey4);
        tieredIndexService.logTieredIndexService();
        Assert.assertEquals(1, tieredIndexService.getNeedCompactOrUploadList().size());
        tieredIndexService.doScheduleCompactOrUpload(tieredIndexService.getNeedCompactOrUploadList());
        Assert.assertEquals(0, tieredIndexService.getNeedCompactOrUploadList().size());
        ConcurrentSkipListMap<Long, IndexFileAccess> concurrentSkipListMap = tieredIndexService.getConcurrentSkipListMap();
        IndexFileAccess indexFileAccess = tieredIndexService.getOrCreateWritingIndexFileAccess(4000);
        //Assert.assertEquals(IndexStatusEnum.UPLOAD_SUCCESS, indexFileAccess.getIndexStatus());
        tieredIndexService.logTieredIndexService();
    }

    @Test
    public void queryAsync() throws ExecutionException, InterruptedException, IOException {
        AppendResult mykey = tieredIndexService.putKey(mq, 22, "mykey1", 22, 3, 1000);
        Assert.assertEquals(AppendResult.SUCCESS, mykey);
        AppendResult mykey2 = tieredIndexService.putKey(mq, 22, "mykey2", 22, 3, 2000);
        Assert.assertEquals(AppendResult.SUCCESS, mykey2);
        AppendResult mykey3 = tieredIndexService.putKey(mq, 22, "mykey3", 22, 3, 3000);
        Assert.assertEquals(AppendResult.SUCCESS, mykey3);
        AppendResult mykey4 = tieredIndexService.putKey(mq, 22, "mykey4", 22, 3, 4000);
        Assert.assertEquals(AppendResult.SUCCESS, mykey4);
        AppendResult mykey5 = tieredIndexService.putKey(mq, 22, "mykey5", 22, 3, 5000);
        Assert.assertEquals(AppendResult.SUCCESS, mykey5);
        tieredIndexService.logTieredIndexService();
        Assert.assertEquals(1, tieredIndexService.getNeedCompactOrUploadList().size());
        tieredIndexService.doScheduleCompactOrUpload(tieredIndexService.getNeedCompactOrUploadList());

        List<IndexItem> queryItems = tieredIndexService.queryAsync(mq.getTopic(), "mykey1", 0, 1000).get();
        for (IndexItem it : queryItems) {
            logger.info("hashCode:{},topicId:{},queueId:{}, offset:{},size:{}, timeDiff:{}", it.getHashCode(), it.getTopicId(), it.getQueueId(), it.getOffset(), it.getSize(), it.getTimeDiff());
        }

        List<IndexItem> queryItems2 = tieredIndexService.queryAsync(mq.getTopic(), "mykey4", 0, 4000).get();
        for (IndexItem it : queryItems2) {
            logger.info("hashCode:{},topicId:{},queueId:{}, offset:{},size:{}, timeDiff:{}", it.getHashCode(), it.getTopicId(), it.getQueueId(), it.getOffset(), it.getSize(), it.getTimeDiff());
        }
    }

    @Test
    public void buildKey() {
    }

    @Test
    public void cleanExpiredFile() {
    }

    @Test
    public void destroyExpiredFile() {
    }

    @Test
    public void destroy() {
        this.tieredIndexService.destroy();
    }
}
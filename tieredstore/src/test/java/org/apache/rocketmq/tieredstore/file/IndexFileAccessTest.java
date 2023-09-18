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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.TieredStoreTestUtil;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexFileAccessTest {

    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private final String storePath = "aaaa";
    private MessageQueue mq;
    private TieredMessageStoreConfig storeConfig;

    private int hashSlotNum = 1;
    private int indexNum = 4;

    IndexFileAccess indexFileAccess;

    TieredFlatFile tieredFlatFile;

    TieredFileAllocator tieredFileAllocator;

    @Before
    public void setUp() throws Exception {
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setBrokerName("IndexFileBroker");
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.provider.posix.PosixFileSegment");
        storeConfig.setTieredStoreIndexFileMaxHashSlotNum(5);
        storeConfig.setTieredStoreIndexFileMaxIndexNum(20);
        mq = new MessageQueue("IndexFileTest", storeConfig.getBrokerName(), 1);
        TieredStoreUtil.getMetadataStore(storeConfig);
        TieredStoreExecutor.init();

    }

    @After
    public void tearDown() {
        if (this.indexFileAccess != null) {
            this.indexFileAccess.destory();
        }
        if (this.tieredFlatFile != null) {
            this.tieredFlatFile.destroy();
        }
        TieredStoreTestUtil.destroyMetadataStore();
        TieredStoreTestUtil.destroyTempDir(storePath);
        TieredStoreExecutor.shutdown();
    }

    @Test
    public void initOrRecoverFromWritingFile() throws IOException {
        indexFileAccess = new IndexFileAccess("writing/000111", hashSlotNum, indexNum, 0, null);

    }

    @Test
    public void initOrrecoverFromComPactFile() throws IOException {
        indexFileAccess = new IndexFileAccess("compact/111222", hashSlotNum, indexNum, 0, null);

    }

    public void recoverFromOssFile() throws ClassNotFoundException, NoSuchMethodException, IOException {
        this.tieredFileAllocator = new TieredFileAllocator(storeConfig);
        this.tieredFlatFile = tieredFileAllocator.createFlatFileForIndexFile(storePath);
        if (tieredFlatFile.getBaseOffset() == -1) {
            tieredFlatFile.setBaseOffset(0);
        }
        indexFileAccess = new IndexFileAccess("oss/222222", hashSlotNum, indexNum, 0, tieredFlatFile.getFileByIndex(tieredFlatFile.getFileSegmentCount() - 1));

    }

    @Test
    public void recoverMappedFile() {
    }

    @Test
    public void setIndexFileStatus() {
    }

    @Test
    public void indexKeyHashMethod() {
    }

    private void printOriginFile(ByteBuffer mappedByteBuffer) {
        logger.info("OriginFile*****\n indexHeader:{},{},{},{},{}", mappedByteBuffer.getInt(0), mappedByteBuffer.getLong(4), mappedByteBuffer.getLong(12), mappedByteBuffer.getInt(20), mappedByteBuffer.getInt(24));
        logger.info("Slots:");
        int baseIndex = 0;
        for (int i = 0; i < hashSlotNum; i++) {
            baseIndex = 28 + 8 * i;
            logger.info("{}:{},", i, mappedByteBuffer.getInt(baseIndex));
        }
        logger.info("indexItems:");
        for (int i = 0; i < indexNum; i++) {
            baseIndex = 28 + 8 * hashSlotNum + 32 * i;
            logger.info("{}:{},{},{},{},{},{},{}", i, mappedByteBuffer.getInt(baseIndex), mappedByteBuffer.getInt(baseIndex + 4), mappedByteBuffer.getInt(baseIndex + 4 + 4), mappedByteBuffer.getLong(baseIndex + 4 + 4 + 4), mappedByteBuffer.getInt(baseIndex + 4 + 4 + 4 + 8), mappedByteBuffer.getInt(baseIndex + 4 + 4 + 4 + 8 + 4), mappedByteBuffer.getInt(baseIndex + 4 + 4 + 4 + 8 + 4 + 4));
        }
        logger.info("endMagicCode Match:{}", mappedByteBuffer.getInt(28 + 8 * hashSlotNum + 32 * indexNum) == TieredIndexHeader.END_MAGIC_CODE);
    }

    private void printCompactFile(ByteBuffer mappedByteBuffer) {
        logger.info("CompactFile*****\n indexHeader:{},{},{},{},{}", mappedByteBuffer.getInt(0), mappedByteBuffer.getLong(4), mappedByteBuffer.getLong(12), mappedByteBuffer.getInt(20), mappedByteBuffer.getInt(24));
        logger.info("Slots:");
        int baseIndex = 0;
        for (int i = 0; i < hashSlotNum; i++) {
            baseIndex = 28 + 8 * i;
            logger.info("{}:{},{}", i, mappedByteBuffer.getInt(baseIndex), mappedByteBuffer.getInt(baseIndex + 4));
        }
        logger.info("indexItems:");
        for (int i = 0; i < mappedByteBuffer.getInt(24) - 1; i++) {
            baseIndex = 28 + 8 * hashSlotNum + 28 * i;
            logger.info("{}:{},{},{},{},{},{}", i, mappedByteBuffer.getInt(baseIndex), mappedByteBuffer.getInt(baseIndex + 4), mappedByteBuffer.getInt(baseIndex + 4 + 4), mappedByteBuffer.getLong(baseIndex + 4 + 4 + 4), mappedByteBuffer.getInt(baseIndex + 4 + 4 + 4 + 8), mappedByteBuffer.getInt(baseIndex + 4 + 4 + 4 + 8 + 4));

        }
    }

    private void printUploadedCompactFile(ByteBuffer mappedByteBuffer) {
        logger.info("uploadedCompactFile*****\n indexHeader:{},{},{},{},{}", mappedByteBuffer.getInt(0), mappedByteBuffer.getLong(4), mappedByteBuffer.getLong(12), mappedByteBuffer.getInt(20), mappedByteBuffer.getInt(24));
        logger.info("Slots:");
        int baseIndex = 0;
        for (int i = 0; i < hashSlotNum; i++) {
            baseIndex = 28 + 8 * i;
            logger.info("{}:{},{}", i, mappedByteBuffer.getInt(baseIndex), mappedByteBuffer.getInt(baseIndex + 4));
        }
        logger.info("indexItems:");
        for (int i = 0; i < mappedByteBuffer.getInt(24) - 1; i++) {
            baseIndex = 28 + 8 * hashSlotNum + 28 * i;
            logger.info("{}:{},{},{},{},{},{}", i, mappedByteBuffer.getInt(baseIndex), mappedByteBuffer.getInt(baseIndex + 4), mappedByteBuffer.getInt(baseIndex + 4 + 4), mappedByteBuffer.getLong(baseIndex + 4 + 4 + 4), mappedByteBuffer.getInt(baseIndex + 4 + 4 + 4 + 8), mappedByteBuffer.getInt(baseIndex + 4 + 4 + 4 + 8 + 4));

        }
    }

    public void putKey() {
        IndexItem indexItem = new IndexItem(111, 123, 1, 124, 34, 12);
        indexFileAccess.putKey("aaa", indexItem, System.currentTimeMillis());
        printOriginFile(indexFileAccess.getByteBuffer());
    }

    public void putKey2() {
        IndexItem indexItem = new IndexItem(112, 124, 2, 111, 5, 12);
        indexFileAccess.putKey("aab", indexItem, System.currentTimeMillis());
        printOriginFile(indexFileAccess.getByteBuffer());

    }

    public void putKey3() {
        IndexItem indexItem = new IndexItem(113, 125, 3, 122, 67, 12);
        indexFileAccess.putKey("aab", indexItem, System.currentTimeMillis());
        printOriginFile(indexFileAccess.getByteBuffer());

    }

    public void putKey4() {
        IndexItem indexItem = new IndexItem(33, 22, 4, 55, 6, 8);
        indexFileAccess.putKey("aab", indexItem, System.currentTimeMillis());
        printOriginFile(indexFileAccess.getByteBuffer());

    }

    @Test
    public void putAll() throws IOException, InterruptedException {
        initOrRecoverFromWritingFile();
        putKey();
        Thread.sleep(1200);
        putKey2();
        Thread.sleep(1100);
        putKey3();
        Thread.sleep(1500);
    }

    @Test
    public void putFileFull() throws IOException, InterruptedException {
        initOrRecoverFromWritingFile();
        putKey();
        assertFalse(this.indexFileAccess.isFileFull());
        Thread.sleep(1200);

        putKey2();
        assertFalse(this.indexFileAccess.isFileFull());
        Thread.sleep(1100);

        putKey3();
        assertTrue(this.indexFileAccess.isFileFull());
        Thread.sleep(1500);

        putKey4();
        assertTrue(this.indexFileAccess.isFileFull());

    }

    @Test
    public void creatAncCompactFile() throws IOException, InterruptedException {
        putFileFull();
        printOriginFile(indexFileAccess.getByteBuffer());
        indexFileAccess.compactFile();
        printCompactFile(indexFileAccess.getByteBuffer());
    }

    @Test
    public void uploadCompactFile() throws IOException, ClassNotFoundException, NoSuchMethodException, InterruptedException {
        putFileFull();
        indexFileAccess.compactFile();
        this.tieredFileAllocator = new TieredFileAllocator(storeConfig);
        this.tieredFlatFile = tieredFileAllocator.createFlatFileForIndexFile(storePath);
        if (tieredFlatFile.getBaseOffset() == -1) {
            tieredFlatFile.setBaseOffset(0);
        }

        indexFileAccess.upLoadCompactFile(tieredFlatFile);
        TieredFileSegment tieredFileSegment = indexFileAccess.getTieredFileSegment();
        ByteBuffer read = tieredFileSegment.read(0, 28 + 8 * this.hashSlotNum + 28 * this.indexNum);
        printUploadedCompactFile(read);
    }

    @Test
    public void testCompactAndUploadFile() throws IOException, ClassNotFoundException, NoSuchMethodException, InterruptedException {
        putFileFull();
        this.tieredFileAllocator = new TieredFileAllocator(storeConfig);
        this.tieredFlatFile = tieredFileAllocator.createFlatFileForIndexFile(storePath);
        if (tieredFlatFile.getBaseOffset() == -1) {
            tieredFlatFile.setBaseOffset(0);
        }
        indexFileAccess.compactOrUpload(tieredFlatFile);
        TieredFileSegment tieredFileSegment = indexFileAccess.getTieredFileSegment();
        ByteBuffer read = tieredFileSegment.read(0, 28 + 8 * this.hashSlotNum + 28 * this.indexNum);
        printUploadedCompactFile(read);
    }

    @Test
    public void queryAsyncFromWritingMappedFile() throws ExecutionException, InterruptedException, IOException {
        putAll();
        printOriginFile(indexFileAccess.getByteBuffer());
        List<IndexItem> aab = indexFileAccess.queryAsync("aab", 0, System.currentTimeMillis()).get();
        for (int i = 0; i < aab.size(); i++) {
            IndexItem it = aab.get(i);
            logger.info("hashCode:{},topicId:{},queueId:{}, offset:{}, timeDiff:{}", it.getHashCode(), it.getTopicId(), it.getQueueId(), it.getOffset(), it.getSize(), it.getTimeDiff());
        }
    }

    @Test
    public void queryAsyncFromCompactedMappedFile() throws ClassNotFoundException, NoSuchMethodException, IOException, ExecutionException, InterruptedException {
        putFileFull();
        printOriginFile(indexFileAccess.getByteBuffer());
        indexFileAccess.compactFile();
        printCompactFile(indexFileAccess.getByteBuffer());
        List<IndexItem> aab = indexFileAccess.queryAsync("aab", 0, System.currentTimeMillis()).get();
        for (int i = 0; i < aab.size(); i++) {
            IndexItem it = aab.get(i);
            logger.info("hashCode:{},topicId:{},queueId:{}, offset:{},size:{}, timeDiff:{}", it.getHashCode(), it.getTopicId(), it.getQueueId(), it.getOffset(), it.getSize(), it.getTimeDiff());
        }
    }

    @Test
    public void queryAsyncFromOssSegmentFile() throws ClassNotFoundException, NoSuchMethodException, IOException, ExecutionException, InterruptedException {
        putFileFull();
        this.tieredFileAllocator = new TieredFileAllocator(storeConfig);
        this.tieredFlatFile = tieredFileAllocator.createFlatFileForIndexFile(storePath);
        if (tieredFlatFile.getBaseOffset() == -1) {
            tieredFlatFile.setBaseOffset(0);
        }
        indexFileAccess.compactOrUpload(this.tieredFlatFile);
        printUploadedCompactFile(indexFileAccess.getTieredFileSegment().read(0, 28 + 8 * this.hashSlotNum + 28 * this.indexNum));
        List<IndexItem> aab = indexFileAccess.queryAsync("aab", 0, System.currentTimeMillis()).get();
        for (int i = 0; i < aab.size(); i++) {
            IndexItem it = aab.get(i);
            logger.info("hashCode:{},topicId:{},queueId:{}, offset:{},size:{}, timeDiff:{}", it.getHashCode(), it.getTopicId(), it.getQueueId(), it.getOffset(), it.getSize(), it.getTimeDiff());
        }
    }

}
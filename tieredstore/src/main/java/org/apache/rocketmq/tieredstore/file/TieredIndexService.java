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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class TieredIndexService {
    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private ConcurrentSkipListMap<Long, IndexFileAccess> concurrentSkipListMap;
    private TieredFileAllocator fileQueueFactory;

    private final TieredMessageStoreConfig storeConfig;
    private TieredFlatFile flatFile;

    private final int maxHashSlotNum;
    private final int maxIndexNum;
    private Long compactIndexFileAccessKey;

    private CompactAndUploadService compactAndUploadService;

    ReentrantLock reentrantLock;

    public TieredIndexService(TieredFileAllocator fileQueueFactory, String filePath) throws IOException {
        System.out.println("*****construct TieredIndexService");
        this.storeConfig = fileQueueFactory.getStoreConfig();
        this.maxHashSlotNum = storeConfig.getTieredStoreIndexFileMaxHashSlotNum();
        this.maxIndexNum = storeConfig.getTieredStoreIndexFileMaxIndexNum();
        this.fileQueueFactory = fileQueueFactory;
        this.concurrentSkipListMap = new ConcurrentSkipListMap<>();
        initOrRecover(filePath, IndexFileAccess.WRITING, IndexFileAccess.COMPACTED);
        this.compactAndUploadService = new CompactAndUploadService(this);
        //this.compactAndUploadService.start();
        reentrantLock = new ReentrantLock();
    }

    private void initOrRecover(String ossfilePath, String writingPath, String compactPath) throws IOException {
        initFromOss(ossfilePath);
        recoverFromLocalPath(writingPath);
        recoverFromLocalPath(compactPath);
        //if there is no writing to recover

    }

    private void initFromOss(String ossFilePath) throws IOException {
        this.flatFile = fileQueueFactory.createFlatFileForIndexFile(ossFilePath);
        if (flatFile.getBaseOffset() == -1) {
            flatFile.setBaseOffset(0);
        }
        for (int i = 0; i < flatFile.getFileSegmentList().size(); i++) {
            IndexFileAccess indexFileAccess = new IndexFileAccess("oss/" + System.currentTimeMillis(), this.maxHashSlotNum, this.maxIndexNum, 0, flatFile.getFileByIndex(i));
            concurrentSkipListMap.put(indexFileAccess.getTieredIndexHeader().getBeginTimestamp(), indexFileAccess);
        }

        if (!concurrentSkipListMap.isEmpty()) {
            compactIndexFileAccessKey = concurrentSkipListMap.lastKey();
        }
    }

    private void recoverFromLocalPath(String localPath) {
        File dir = new File(localPath);
        File[] files = dir.listFiles();
        if (files != null && files.length != 0) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {
                try {
                    IndexFileAccess f = new IndexFileAccess(localPath + "/" + file.getName(), this.maxHashSlotNum, this.maxIndexNum, 0, null);
                    concurrentSkipListMap.put(f.getTieredIndexHeader().getBeginTimestamp(), f);
                } catch (IOException | NumberFormatException e) {
                    logger.error("load file {} error", file, e);
                }
            }
        }
    }

    public AppendResult putKey(MessageQueue mq, int topicId, String key, long offset, int size, long timeStamp) {
        IndexItem indexItem = new IndexItem(topicId, mq.getQueueId(), offset, size);
        IndexFileAccess indexFileAccess = getOrCreateWritingIndexFileAccess(timeStamp);
        if (indexFileAccess == null) {
            return AppendResult.IO_ERROR;
        }
        return indexFileAccess.putKey(buildKey(mq.getTopic(), key), indexItem, timeStamp);
    }

    public AppendResult putBatchKeys(MessageQueue mq, int topicId, String[] keys, long offset, int size,
        long timeStamp) {
        for (String key : keys) {
            if (StringUtils.isBlank(key)) {
                continue;
            }
            AppendResult result = putKey(mq, topicId, key, offset, size, timeStamp);
            if (AppendResult.SUCCESS != result) {
                return result;
            }
        }
        return AppendResult.SUCCESS;
    }

    private boolean needCreateWritingIndexFileAccess() {
        if (concurrentSkipListMap.isEmpty()) {
            return true;
        } else {
            IndexFileAccess lastIndexFileAccess = concurrentSkipListMap.lastEntry().getValue();
            if (lastIndexFileAccess.getIndexStatus() != IndexStatusEnum.WRITING) {
                return true;
            } else
                return lastIndexFileAccess.getIndexStatus() == IndexStatusEnum.WRITING && lastIndexFileAccess.isFileFull();
        }
    }

    public IndexFileAccess getOrCreateWritingIndexFileAccess(long timeStamp) {
        //double check
        if (needCreateWritingIndexFileAccess()) {
            reentrantLock.lock();
            try {
                if (needCreateWritingIndexFileAccess()) {
                    IndexFileAccess newIndexFileAccess = new IndexFileAccess("writing/" + timeStamp, maxHashSlotNum, maxIndexNum, timeStamp, null);
                    concurrentSkipListMap.put(timeStamp, newIndexFileAccess);
                    //init last compact position
                    if (compactIndexFileAccessKey == null) {
                        compactIndexFileAccessKey = concurrentSkipListMap.firstKey();
                    }
                }
            } catch (IOException e) {
                logger.error("create IndexFileAccess error");
            } finally {
                reentrantLock.unlock();
            }
        }
        return concurrentSkipListMap.lastEntry().getValue();
    }

    public CompletableFuture<List<IndexItem>> queryAsync(String topic, String key, long beginTime,
        long endTime) {
        //find IndexFileAccess range log(M)
        ConcurrentNavigableMap<Long, IndexFileAccess> subSkipListMap = concurrentSkipListMap.subMap(beginTime, true, endTime, true);
        CompletableFuture<List<IndexItem>> future = CompletableFuture.supplyAsync(ArrayList::new);
        for (Long keyTime : subSkipListMap.keySet()) {
            IndexFileAccess indexFileAccess = subSkipListMap.get(keyTime);
            //find indexItem in indexFileAccess
            CompletableFuture<List<IndexItem>> indexItemsFuture = indexFileAccess.queryAsync(buildKey(topic, key), beginTime, endTime);
            future = future.thenCombine(indexItemsFuture, (retList, tempList) -> {
                retList.addAll(tempList);
                return retList;
            });
        }
        return future;
    }

    public void doScheduleCompactOrUpload(List<Long> needCompactAndUploadList) throws IOException {
        for (Long timeStamp : needCompactAndUploadList) {
            IndexFileAccess indexFileAccess = concurrentSkipListMap.get(timeStamp);
            indexFileAccess.compactOrUpload(this.flatFile);
            compactIndexFileAccessKey = timeStamp;
        }
    }

    public List<Long> getNeedCompactOrUploadList() {
        List<Long> needCompactOrUploadList = new ArrayList<>();
        if (compactIndexFileAccessKey == null) {
            return needCompactOrUploadList;
        }
        ConcurrentNavigableMap<Long, IndexFileAccess> subMap = concurrentSkipListMap.tailMap(compactIndexFileAccessKey);
        for (Long timeStamp : subMap.keySet()) {
            IndexFileAccess indexFileAccess = subMap.get(timeStamp);
            if (!indexFileAccess.isFileFull()) {
                continue;
            }
            if (IndexStatusEnum.WRITING == indexFileAccess.getIndexStatus() || IndexStatusEnum.COMPACTED == indexFileAccess.getIndexStatus()) {
                needCompactOrUploadList.add(timeStamp);
            }
        }
        return needCompactOrUploadList;
    }

    public static String buildKey(String topic, String key) {
        return topic + "#" + key;
    }

    public void cleanExpiredFile(long expireTimestamp) {
        flatFile.cleanExpiredFile(expireTimestamp);
    }

    public void destroyExpiredFile() {
        flatFile.destroyExpiredFile();
    }

    public void commit(boolean sync) {
        flatFile.commit(sync);
    }

    public void destroy() {
        for (Long key : concurrentSkipListMap.keySet()) {
            IndexFileAccess indexFileAccess = concurrentSkipListMap.get(key);
            if (IndexStatusEnum.WRITING == indexFileAccess.getIndexStatus() || IndexStatusEnum.COMPACTED == indexFileAccess.getIndexStatus()) {
                indexFileAccess.destory();
            }
        }
        if (flatFile != null) {
            flatFile.destroy();
        }
        compactAndUploadService.shutdown();
    }

    //for test
    public ConcurrentSkipListMap<Long, IndexFileAccess> getConcurrentSkipListMap() {
        return concurrentSkipListMap;
    }

    public Long getCompactIndexFileAccessKey() {
        return compactIndexFileAccessKey;
    }

    public void logTieredIndexService() {
        for (Long key : this.concurrentSkipListMap.keySet()) {
            IndexFileAccess indexFileAccess = this.concurrentSkipListMap.get(key);
            indexFileAccess.logIndexFileAccess();
        }
    }

    static class CompactAndUploadService extends ServiceThread {

        private TieredIndexService tieredIndexService;

        public CompactAndUploadService(TieredIndexService tieredIndexService) {
            this.tieredIndexService = tieredIndexService;
        }

        @Override
        public String getServiceName() {
            return this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            while (!this.isStopped()) {
                try {
                    List<Long> needCompactOrUploadList = tieredIndexService.getNeedCompactOrUploadList();
                    if (CollectionUtils.isEmpty(needCompactOrUploadList)) {
                        waitForRunning(10000);
                        continue;
                    }
                    tieredIndexService.doScheduleCompactOrUpload(needCompactOrUploadList);
                } catch (Throwable e) {
                    logger.error("Error occurred in " + getServiceName(), e);
                }
            }
        }
    }

}

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
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

/**
 * a single IndexFile in indexService
 */
public class IndexFileAccess {

    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);
    // header format:
    // magic code(4) + begin timestamp(8) + end timestamp(8) + slot num(4) + index num(4)
    public static final String WRITING = "writing";
    public static final String COMPACTED = "compact";

    public static final int HASH_SLOT_SIZE = 8;

    public static final int ORIGIN_INDEX_ITEM_SIZE = 32;
    public static final int COMPACT_INDEX_ITEM_SIZE = 28;

    private static final int INVALID_INDEX = 0;

    // index item

    private AtomicReference<IndexStatusEnum> indexStatus;

    private MappedFile writingMappedFile;

    private ByteBuffer byteBuffer;

    private MappedFile compactedMappedFile;

    private TieredFileSegment tieredFileSegment;

    private ReadWriteLock readWriteLock;

    private int fileMaxSize;

    private final int maxHashSlotNum;
    private final int maxIndexNum;

    private TieredIndexHeader tieredIndexHeader;

    private String fileName;

    public IndexFileAccess(final String fileName, final int hashSlotNum, final int indexNum, final long endTimestamp,
        TieredFileSegment tieredFileSegment) throws IOException {
        readWriteLock = new ReentrantReadWriteLock();
        this.fileName = fileName;
        String[] splitNames = fileName.split("/");
        this.maxHashSlotNum = hashSlotNum;
        this.maxIndexNum = indexNum;
        if (splitNames[0].equals(WRITING)) {
            //recover or init from writing mapped file
            this.writingMappedFile = initOrRecoverMappedFile(fileName, endTimestamp, HASH_SLOT_SIZE, ORIGIN_INDEX_ITEM_SIZE, IndexStatusEnum.WRITING);
        } else if (splitNames[0].equals(COMPACTED)) {
            //recover  from compact mapped file
            this.compactedMappedFile = initOrRecoverMappedFile(fileName, endTimestamp, HASH_SLOT_SIZE, COMPACT_INDEX_ITEM_SIZE, IndexStatusEnum.COMPACTED);
        } else {
            //recover  from oss segment file
            this.tieredFileSegment = tieredFileSegment;
            ByteBuffer headBuffer = tieredFileSegment.read(0, TieredIndexHeader.INDEX_HEADER_SIZE);
            this.tieredIndexHeader = new TieredIndexHeader();
            this.tieredIndexHeader.initOrRecoverFromByteBuffer(headBuffer);
            if (endTimestamp > 0) {
                this.tieredIndexHeader.setBeginTimestamp(endTimestamp);
                this.tieredIndexHeader.setEndTimestamp(endTimestamp);
                this.tieredIndexHeader.updateByteBufferHeader(headBuffer);
            }
            this.fileMaxSize = TieredIndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * HASH_SLOT_SIZE) + (tieredIndexHeader.getIndexCount() * COMPACT_INDEX_ITEM_SIZE) + 4;
            this.byteBuffer = tieredFileSegment.read(0, fileMaxSize);//?????
            this.indexStatus = new AtomicReference<>(IndexStatusEnum.UPLOAD_SUCCESS);

        }
    }

    MappedFile initOrRecoverMappedFile(final String fileName, final long endTimestamp, final int slotSize,
        final int indexItemSize, IndexStatusEnum status) throws IOException {
        this.fileMaxSize = TieredIndexHeader.INDEX_HEADER_SIZE + (maxHashSlotNum * slotSize) + (maxIndexNum * indexItemSize) + 4;
        MappedFile mappedFile = new DefaultMappedFile(fileName, fileMaxSize);
        this.byteBuffer = mappedFile.getMappedByteBuffer();
        this.tieredIndexHeader = new TieredIndexHeader();
        this.tieredIndexHeader.initOrRecoverFromByteBuffer(this.byteBuffer);
        if (endTimestamp > 0) {
            this.tieredIndexHeader.setBeginTimestamp(endTimestamp);
            this.tieredIndexHeader.setEndTimestamp(endTimestamp);
            this.tieredIndexHeader.updateByteBufferHeader(this.byteBuffer);
        }
        this.indexStatus = new AtomicReference<>(status);
        return mappedFile;
    }

    public void setIndexFileStatus(IndexStatusEnum status) {
        indexStatus.updateAndGet(oldStatus -> status);
    }

    public int indexKeyHashMethod(String key) {
        int keyHash = key.hashCode();
        return (keyHash < 0) ? -keyHash : keyHash;
    }

    private static boolean isFileSealed(MappedFile mappedFile) {
        return mappedFile.getMappedByteBuffer().getInt(mappedFile.getFileSize() - 4) == TieredIndexHeader.END_MAGIC_CODE;
    }

    public boolean isFileFull() {
        return this.tieredIndexHeader.getIndexCount() >= this.maxIndexNum;
    }

    public AppendResult putKey(final String key, final IndexItem indexItem, final long storeTimestamp) {
        readWriteLock.writeLock().lock();
        try {
            if (this.indexStatus.get() != IndexStatusEnum.WRITING) {
                return AppendResult.UNKNOWN_ERROR;
            }
            if (isFileFull()) {
                return AppendResult.FILE_FULL;
            }

            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.maxHashSlotNum;
            int absSlotPos = TieredIndexHeader.INDEX_HEADER_SIZE + slotPos * HASH_SLOT_SIZE;
            int slotValue = this.byteBuffer.getInt(absSlotPos);
            if (slotValue <= INVALID_INDEX || slotValue > tieredIndexHeader.getIndexCount()) {
                slotValue = INVALID_INDEX;
            }
            long timeDiff = storeTimestamp - this.tieredIndexHeader.getBeginTimestamp();

            timeDiff = timeDiff / 1000;

            if (this.tieredIndexHeader.getBeginTimestamp() <= 0) {
                timeDiff = 0;
            } else if (timeDiff > Integer.MAX_VALUE) {
                timeDiff = Integer.MAX_VALUE;
            } else if (timeDiff < 0) {
                timeDiff = 0;
            }
            indexItem.setHashCode(keyHash);
            indexItem.setTimeDiff((int) timeDiff);

            int absIndexPos =
                TieredIndexHeader.INDEX_HEADER_SIZE + this.maxHashSlotNum * HASH_SLOT_SIZE
                    + this.tieredIndexHeader.getIndexCount() * ORIGIN_INDEX_ITEM_SIZE;

            indexItem.updateToByteBuffer(this.byteBuffer, absIndexPos);
            byteBuffer.putInt(absIndexPos + IndexItem.INDEX_ITEM_SIZE, slotValue);

            this.byteBuffer.putInt(absSlotPos, this.tieredIndexHeader.getIndexCount());

            //update header
            if (this.tieredIndexHeader.getIndexCount() <= 1) {
                this.tieredIndexHeader.setBeginTimestamp(storeTimestamp);
            }
            if (INVALID_INDEX == slotValue) {
                this.tieredIndexHeader.incHashSlotCount();
            }
            this.tieredIndexHeader.incIndexCount();
            this.tieredIndexHeader.setEndTimestamp(storeTimestamp);
            this.tieredIndexHeader.updateByteBufferHeader(this.byteBuffer);
            if (this.tieredIndexHeader.getIndexCount() == maxIndexNum) {
                this.byteBuffer.putInt(fileMaxSize - 4, TieredIndexHeader.END_MAGIC_CODE);
            }

            return AppendResult.SUCCESS;
        } catch (Exception e) {
            logger.error("TieredIndexFile#putKey: put key failed:", e);
        } finally {
            readWriteLock.writeLock().unlock();
        }
        return AppendResult.IO_ERROR;
    }

    public CompletableFuture<List<IndexItem>> queryAsync(final String key,
        final long begin, final long end) {
        this.readWriteLock.readLock().lock();
        try {
            if (this.indexStatus.get() == IndexStatusEnum.WRITING) {
                //read from writingMappedFile
                logger.info(this.fileName + " status is WRITING, read from WritingMappedFile");
                return queryAsyncFromWritingMappedFile(key, begin, end);
            } else if (this.indexStatus.get() == IndexStatusEnum.COMPACTED) {
                // read from compactedMappedFile
                logger.info(this.fileName + " status is COMPACTED, read from CompactedMappedFile");
                return queryAsyncFromCompactedMappedFile(key, begin, end);

            } else if (this.indexStatus.get() == IndexStatusEnum.UPLOAD_SUCCESS) {
                // read from tieredFileSegment
                logger.info(this.fileName + " status is UPLOAD_SUCCESS, read from UploadedFile");
                return queryAsyncFromUploadedFile(key, begin, end);

            }
            return new CompletableFuture<>();
        } catch (Exception e) {
            return new CompletableFuture<>();
        } finally {
            this.readWriteLock.readLock().unlock();
        }
    }

    public CompletableFuture<List<IndexItem>> queryAsyncFromWritingMappedFile(final String key,
        final long begin, final long end) {
        return CompletableFuture.supplyAsync(() -> {
            List<IndexItem> retList = new ArrayList<>();
            if (!this.writingMappedFile.hold()) {
                return retList;
            }
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.maxHashSlotNum;
            int absSlotPos = TieredIndexHeader.INDEX_HEADER_SIZE + slotPos * HASH_SLOT_SIZE;

            try {
                int slotValue = this.byteBuffer.getInt(absSlotPos);
                if (slotValue <= INVALID_INDEX || slotValue > this.tieredIndexHeader.getIndexCount()
                    || this.tieredIndexHeader.getIndexCount() <= 1) {
                    return retList;
                }
                for (int nextIndexToRead = slotValue, i = 0; ; ) {
                    int absIndexPos =
                        TieredIndexHeader.INDEX_HEADER_SIZE + this.maxHashSlotNum * HASH_SLOT_SIZE
                            + nextIndexToRead * ORIGIN_INDEX_ITEM_SIZE;
                    IndexItem indexItem = new IndexItem();
                    indexItem.loadFromByteBuffer(this.byteBuffer, absIndexPos);

                    int prevIndexRead = this.byteBuffer.getInt(absIndexPos + IndexItem.INDEX_ITEM_SIZE);

                    int timeDiff = indexItem.getTimeDiff();
                    if (timeDiff < 0) {
                        break;
                    }
                    timeDiff *= 1000L;
                    long timeRead = this.tieredIndexHeader.getBeginTimestamp() + timeDiff;
                    boolean timeMatched = timeRead >= begin && timeRead <= end;

                    if (keyHash == indexItem.getHashCode() && timeMatched) {
                        retList.add(indexItem);
                    }

                    if (prevIndexRead <= INVALID_INDEX
                        || prevIndexRead > this.tieredIndexHeader.getIndexCount()
                        || prevIndexRead == nextIndexToRead || timeRead < begin || i > this.tieredIndexHeader.getIndexCount()) {
                        break;
                    }

                    nextIndexToRead = prevIndexRead;
                    i++;
                }
            } catch (Exception e) {
                logger.error("queryAsyncFromWritingMappedFile exception ", e);
            } finally {
                this.writingMappedFile.release();
            }

            return retList;
        });

    }

    public CompletableFuture<List<IndexItem>> queryAsyncFromCompactedMappedFile(final String key,
        final long begin, final long end) {
        return CompletableFuture.supplyAsync(() -> {
            List<IndexItem> retList = new ArrayList<>();
            if (!this.compactedMappedFile.hold()) {
                return retList;
            }
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.maxHashSlotNum;
            int absSlotPos = TieredIndexHeader.INDEX_HEADER_SIZE + slotPos * HASH_SLOT_SIZE;
            try {
                int slotValue = this.byteBuffer.getInt(absSlotPos);
                int indexTotalSize = this.byteBuffer.getInt(absSlotPos + 4);
                if (slotValue <= INVALID_INDEX || indexTotalSize <= 0) {
                    return retList;
                }
                for (int nextAbsPos = slotValue; nextAbsPos < slotValue + indexTotalSize; nextAbsPos += COMPACT_INDEX_ITEM_SIZE) {
                    IndexItem indexItem = new IndexItem();
                    indexItem.loadFromByteBuffer(this.byteBuffer, nextAbsPos);
                    int timeDiff = indexItem.getTimeDiff();
                    if (timeDiff < 0) {
                        break;
                    }
                    timeDiff *= 1000L;
                    long timeRead = this.tieredIndexHeader.getBeginTimestamp() + timeDiff;
                    boolean timeMatched = timeRead >= begin && timeRead <= end;

                    if (keyHash == indexItem.getHashCode() && timeMatched) {
                        retList.add(indexItem);
                    }
                }
            } catch (Exception e) {
                logger.error("queryAsyncFromWritingMappedFile exception ", e);
            } finally {
                this.compactedMappedFile.release();
            }
            return retList;
        });
    }

    public CompletableFuture<List<IndexItem>> queryAsyncFromUploadedFile(final String key,
        final long begin, final long end) {
        int hashCode = indexKeyHashMethod(key);
        int slotPosition = hashCode % maxHashSlotNum;

        return this.tieredFileSegment.readAsync(TieredIndexHeader.INDEX_HEADER_SIZE + (long) slotPosition * HASH_SLOT_SIZE, HASH_SLOT_SIZE)
            .thenCompose(slotBuffer -> {
                int indexPosition = slotBuffer.getInt();
                int indexTotalSize = slotBuffer.getInt();
                if (indexPosition <= INVALID_INDEX || indexTotalSize <= 0) {
                    return CompletableFuture.completedFuture(null);
                }
                CompletableFuture<ByteBuffer> bufferFuture = this.tieredFileSegment.readAsync(indexPosition, indexTotalSize);
                return bufferFuture.thenApply(buffer -> {
                    List<IndexItem> reslutList = new ArrayList<>();
                    for (int itemIndex = 0; itemIndex < indexTotalSize; itemIndex += COMPACT_INDEX_ITEM_SIZE) {
                        IndexItem indexItem = new IndexItem();
                        indexItem.loadFromByteBuffer(buffer, itemIndex);
                        int timeDiff = indexItem.getTimeDiff();
                        if (timeDiff < 0) {
                            break;
                        }
                        timeDiff *= 1000L;
                        long timeRead = this.tieredIndexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = timeRead >= begin && timeRead <= end;

                        if (hashCode == indexItem.getHashCode() && timeMatched) {
                            reslutList.add(indexItem);
                        }
                    }
                    return reslutList;
                });

            });

    }

    public void compactOrUpload(TieredFlatFile tieredFlatFile) throws IOException {
        if (!isFileFull()) {
            logger.info(this.fileName + " is not full, can not compact And Upload.");
            return;
        }
        if (IndexStatusEnum.WRITING == this.indexStatus.get()) {
            compactFile();
            upLoadCompactFile(tieredFlatFile);
        } else if (IndexStatusEnum.COMPACTED == this.indexStatus.get()) {
            upLoadCompactFile(tieredFlatFile);
        } else {
            logger.error("try to compact or upload a index file which status is WRITING or Compacted");
        }
    }

    public void compactFile() throws IOException {
        if (this.indexStatus.get() != IndexStatusEnum.WRITING) {
            logger.error("try to compact a index file which stats is not writing!");
        }
        this.compactedMappedFile = initOrRecoverMappedFile("compact/111222", 0, HASH_SLOT_SIZE, COMPACT_INDEX_ITEM_SIZE, IndexStatusEnum.WRITING);
        compactByteBuffer();
        this.readWriteLock.readLock().lock();
        this.byteBuffer = this.compactedMappedFile.getMappedByteBuffer();
        this.fileMaxSize = TieredIndexHeader.INDEX_HEADER_SIZE + (this.maxHashSlotNum * HASH_SLOT_SIZE) + (tieredIndexHeader.getIndexCount() * COMPACT_INDEX_ITEM_SIZE) + 4;
        this.writingMappedFile.destroy(-1);
        this.writingMappedFile = null;
        this.setIndexFileStatus(IndexStatusEnum.COMPACTED);
        this.readWriteLock.readLock().unlock();

    }

    public void upLoadCompactFile(TieredFlatFile tieredFlatFile) {
        if (this.indexStatus.get() != IndexStatusEnum.COMPACTED) {
            logger.error("try to compact a index file which stats is not compacted!");
        }
        tieredFlatFile.append(this.compactedMappedFile.getMappedByteBuffer());
        tieredFlatFile.commit(true);
        this.readWriteLock.readLock().lock();
        this.tieredFileSegment = tieredFlatFile.getFileByIndex(tieredFlatFile.getFileSegmentCount() - 1);
        this.fileMaxSize = TieredIndexHeader.INDEX_HEADER_SIZE + (this.maxHashSlotNum * HASH_SLOT_SIZE) + (tieredIndexHeader.getIndexCount() * COMPACT_INDEX_ITEM_SIZE) + 4;
        this.byteBuffer = this.tieredFileSegment.read(0, this.fileMaxSize);
        this.compactedMappedFile.destroy(-1);
        this.compactedMappedFile = null;
        this.setIndexFileStatus(IndexStatusEnum.UPLOAD_SUCCESS);
        this.readWriteLock.readLock().unlock();

    }

    private void compactByteBuffer() {

        MappedByteBuffer originMappedByteBuffer = writingMappedFile.getMappedByteBuffer();
        MappedByteBuffer compactMappedByteBuffer = compactedMappedFile.getMappedByteBuffer();
        this.tieredIndexHeader.loadFromByteBuffer(originMappedByteBuffer);
        this.tieredIndexHeader.updateByteBufferHeader(compactMappedByteBuffer);
        int rePutAbsIndexPos = TieredIndexHeader.INDEX_HEADER_SIZE + (maxHashSlotNum * HASH_SLOT_SIZE);
        for (int i = 0; i < maxHashSlotNum; i++) {
            int absSlotPos = TieredIndexHeader.INDEX_HEADER_SIZE + i * HASH_SLOT_SIZE;
            int slotValue = originMappedByteBuffer.getInt(absSlotPos);
            if (slotValue <= INVALID_INDEX || slotValue > this.tieredIndexHeader.getIndexCount()
                || this.tieredIndexHeader.getIndexCount() <= 1) {
                continue;
            }
            int indexTotalSize = 0;
            for (int nextIndexToRead = slotValue; ; ) {
                int absIndexPos = TieredIndexHeader.INDEX_HEADER_SIZE + maxHashSlotNum * HASH_SLOT_SIZE + nextIndexToRead * ORIGIN_INDEX_ITEM_SIZE;
                int rePutAbsIndexPosCur = rePutAbsIndexPos + indexTotalSize;
                IndexItem indexItem = new IndexItem();
                indexItem.loadFromByteBuffer(originMappedByteBuffer, absIndexPos);
                indexItem.updateToByteBuffer(compactMappedByteBuffer, rePutAbsIndexPosCur);
                indexTotalSize += COMPACT_INDEX_ITEM_SIZE;
                int prevIndexRead = originMappedByteBuffer.getInt(absIndexPos + IndexItem.INDEX_ITEM_SIZE);

                if (prevIndexRead <= INVALID_INDEX || prevIndexRead > this.tieredIndexHeader.getIndexCount()
                    || prevIndexRead == nextIndexToRead) {
                    break;
                }
                nextIndexToRead = prevIndexRead;

            }
            compactMappedByteBuffer.putInt(absSlotPos, rePutAbsIndexPos);
            compactMappedByteBuffer.putInt(absSlotPos + 4, indexTotalSize);
            rePutAbsIndexPos += indexTotalSize;
        }
        compactMappedByteBuffer.putInt(TieredIndexHeader.MAGIC_CODE_INDEX, TieredIndexHeader.END_MAGIC_CODE);
        compactMappedByteBuffer.putInt(rePutAbsIndexPos, TieredIndexHeader.BEGIN_MAGIC_CODE);
        compactMappedByteBuffer.limit(rePutAbsIndexPos + 4);
    }

    //need to be deleted
    public void logIndexFileAccess() {
        logger.info("logIndexFileAccess");
        if (IndexStatusEnum.WRITING == this.indexStatus.get()) {
            logger.info("key:{}, status:{}", this.tieredIndexHeader.getBeginTimestamp(), "WRITING");
            this.tieredIndexHeader.logIndexHeader();
            for (int i = 0; i < this.maxHashSlotNum; i++) {
                logger.info("{}:{}", i, this.byteBuffer.getInt(TieredIndexHeader.INDEX_HEADER_SIZE + i * HASH_SLOT_SIZE));
            }
            for (int i = 0; i < this.maxIndexNum; i++) {
                IndexItem indexItem = new IndexItem();
                indexItem.loadFromByteBuffer(this.byteBuffer, TieredIndexHeader.INDEX_COUNT_INDEX + maxHashSlotNum * HASH_SLOT_SIZE + i * ORIGIN_INDEX_ITEM_SIZE);
                indexItem.logIndexItem();
            }
        } else if (IndexStatusEnum.COMPACTED == this.indexStatus.get()) {
            logger.info("key:{}, status:{}", this.tieredIndexHeader.getBeginTimestamp(), "COMPACTED");
            this.tieredIndexHeader.logIndexHeader();
            for (int i = 0; i < this.maxHashSlotNum; i++) {
                logger.info("{}:{},{}", i, this.byteBuffer.getInt(TieredIndexHeader.INDEX_HEADER_SIZE + i * HASH_SLOT_SIZE), this.byteBuffer.getInt(TieredIndexHeader.INDEX_HEADER_SIZE + i * HASH_SLOT_SIZE + 4));
            }
            for (int i = 0; i < this.tieredIndexHeader.getIndexCount() - 1; i++) {
                IndexItem indexItem = new IndexItem();
                indexItem.loadFromByteBuffer(this.byteBuffer, TieredIndexHeader.INDEX_HEADER_SIZE + maxHashSlotNum * HASH_SLOT_SIZE + i * COMPACT_INDEX_ITEM_SIZE);
                indexItem.logIndexItem();
            }
        } else if (IndexStatusEnum.UPLOAD_SUCCESS == this.indexStatus.get()) {
            logger.info("key:{}, status:{}", this.tieredIndexHeader.getBeginTimestamp(), "UPLOAD_SUCCESS");
            this.tieredIndexHeader.logIndexHeader();
            for (int i = 0; i < this.maxHashSlotNum; i++) {
                logger.info("{}:{},{}", i, this.byteBuffer.getInt(TieredIndexHeader.INDEX_HEADER_SIZE + i * HASH_SLOT_SIZE), this.byteBuffer.getInt(TieredIndexHeader.INDEX_HEADER_SIZE + i * HASH_SLOT_SIZE + 4));
            }
            for (int i = 0; i < this.tieredIndexHeader.getIndexCount() - 1; i++) {
                IndexItem indexItem = new IndexItem();
                indexItem.loadFromByteBuffer(this.byteBuffer, TieredIndexHeader.INDEX_HEADER_SIZE + maxHashSlotNum * HASH_SLOT_SIZE + i * COMPACT_INDEX_ITEM_SIZE);
                indexItem.logIndexItem();
            }
        } else {
            logger.info("ERROR STATUS");

        }
    }

    public String getFileName() {
        return fileName;
    }

    public ByteBuffer getByteBuffer() {
        return this.byteBuffer;
    }

    public IndexStatusEnum getIndexStatus() {
        return this.indexStatus.get();
    }

    public TieredFileSegment getTieredFileSegment() {
        return this.tieredFileSegment;
    }

    public TieredIndexHeader getTieredIndexHeader() {
        return this.tieredIndexHeader;
    }

    public void destory() {
        if (IndexStatusEnum.WRITING == this.getIndexStatus()) {
            this.writingMappedFile.destroy(-1);
        } else if (IndexStatusEnum.COMPACTED == this.getIndexStatus()) {
            this.compactedMappedFile.destroy(-1);
        }
    }

}

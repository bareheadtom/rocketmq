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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class TieredIndexHeader {
    // header format:
    // magic code(4) + begin timestamp(8) + end timestamp(8) + slot num(4) + index num(4)
    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    public static final int INDEX_HEADER_SIZE = 28;

    public static final int MAGIC_CODE_INDEX = 0;
    public static final int BEGIN_TIME_STAMP_INDEX = 4;
    public static final int END_TIME_STAMP_INDEX = 12;
    public static final int SLOT_COUNT_INDEX = 20;
    public static final int INDEX_COUNT_INDEX = 24;

    public static final int BEGIN_MAGIC_CODE = 0xCCDDEEFF ^ 1880681586 + 4;
    public static final int END_MAGIC_CODE = 0xCCDDEEFF ^ 1880681586 + 8;

    private final AtomicInteger magicCode = new AtomicInteger(BEGIN_MAGIC_CODE);
    private final AtomicLong beginTimestamp = new AtomicLong(-1L);
    private final AtomicLong endTimestamp = new AtomicLong(-1L);
    private final AtomicInteger hashSlotCount = new AtomicInteger(0);
    private final AtomicInteger indexCount = new AtomicInteger(1);

    public TieredIndexHeader() {
    }

    public void initOrRecoverFromByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer.getInt(0) != BEGIN_MAGIC_CODE && byteBuffer.getInt(0) != END_MAGIC_CODE) {
            byteBuffer.putInt(MAGIC_CODE_INDEX, BEGIN_MAGIC_CODE);
            byteBuffer.putLong(BEGIN_TIME_STAMP_INDEX, -1L);
            byteBuffer.putLong(END_TIME_STAMP_INDEX, -1L);
            byteBuffer.putInt(SLOT_COUNT_INDEX, 0);
            byteBuffer.putInt(INDEX_COUNT_INDEX, 1);
        } else {
            loadFromByteBuffer(byteBuffer);
        }
    }

    public int getMagicCode() {
        return this.magicCode.get();
    }

    public void loadFromByteBuffer(ByteBuffer byteBuffer) {
        this.magicCode.set(byteBuffer.getInt(MAGIC_CODE_INDEX));
        this.beginTimestamp.set(byteBuffer.getLong(BEGIN_TIME_STAMP_INDEX));
        this.endTimestamp.set(byteBuffer.getLong(END_TIME_STAMP_INDEX));
        this.hashSlotCount.set(byteBuffer.getInt(SLOT_COUNT_INDEX));
        this.indexCount.set(byteBuffer.getInt(INDEX_COUNT_INDEX));

        if (this.indexCount.get() <= 0) {
            this.indexCount.set(1);
        }
    }

    public void updateByteBufferHeader(ByteBuffer byteBuffer) {
        byteBuffer.putInt(MAGIC_CODE_INDEX, this.magicCode.get());
        byteBuffer.putLong(BEGIN_TIME_STAMP_INDEX, this.beginTimestamp.get());
        byteBuffer.putLong(END_TIME_STAMP_INDEX, this.endTimestamp.get());
        byteBuffer.putInt(SLOT_COUNT_INDEX, this.hashSlotCount.get());
        byteBuffer.putInt(INDEX_COUNT_INDEX, this.indexCount.get());
    }

    public long getBeginTimestamp() {
        return beginTimestamp.get();
    }

    public void setBeginTimestamp(long beginTimestamp) {
        this.beginTimestamp.set(beginTimestamp);
    }

    public long getEndTimestamp() {
        return endTimestamp.get();
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp.set(endTimestamp);
    }

    public int getHashSlotCount() {
        return hashSlotCount.get();
    }

    public void incHashSlotCount() {
        int value = this.hashSlotCount.incrementAndGet();
    }

    public int getIndexCount() {
        return indexCount.get();
    }

    public void incIndexCount() {
        int value = this.indexCount.incrementAndGet();
    }

    public void logIndexHeader() {
        logger.info("magicCode:{}, beginTimeStamp:{}, endTimeStamp:{}, hashSlotCount:{}, indexCount:{}", magicCode, beginTimestamp, endTimestamp, hashSlotCount, indexCount);
    }

}

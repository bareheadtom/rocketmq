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
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class IndexItem {
    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private int hashCode;

    private int topicId;

    private int queueId;

    private long offset;

    private int size;

    private int timeDiff;

    public static final int INDEX_ITEM_SIZE = 28;

    public IndexItem() {

    }

    public IndexItem(int topicId, int queueId, long offset, int size) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.offset = offset;
        this.size = size;
    }

    public IndexItem(int hashCode, int topicId, int queueId, long offset, int size, int timeDiff) {
        this.hashCode = hashCode;
        this.topicId = topicId;
        this.queueId = queueId;
        this.offset = offset;
        this.size = size;
        this.timeDiff = timeDiff;
    }

    public void loadFromByteBuffer(ByteBuffer byteBuffer, int baseIndex) {
        hashCode = byteBuffer.getInt(baseIndex);
        topicId = byteBuffer.getInt(baseIndex + 4);
        queueId = byteBuffer.getInt(baseIndex + 4 + 4);
        offset = byteBuffer.getLong(baseIndex + 4 + 4 + 4);
        size = byteBuffer.getInt(baseIndex + 4 + 4 + 4 + 8);
        timeDiff = byteBuffer.getInt(baseIndex + 4 + 4 + 4 + 8 + 4);
    }

    public void updateToByteBuffer(ByteBuffer byteBuffer, int baseIndex) {
        byteBuffer.putInt(baseIndex, hashCode);
        byteBuffer.putInt(baseIndex + 4, topicId);
        byteBuffer.putInt(baseIndex + 4 + 4, queueId);
        byteBuffer.putLong(baseIndex + 4 + 4 + 4, offset);
        byteBuffer.putInt(baseIndex + 4 + 4 + 4 + 8, size);
        byteBuffer.putInt(baseIndex + 4 + 4 + 4 + 8 + 4, timeDiff);
    }

    public int getHashCode() {
        return hashCode;
    }

    public void setHashCode(int hashCode) {
        this.hashCode = hashCode;
    }

    public int getTopicId() {
        return topicId;
    }

    public void setTopicId(int topicId) {
        this.topicId = topicId;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getTimeDiff() {
        return timeDiff;
    }

    public void setTimeDiff(int timeDiff) {
        this.timeDiff = timeDiff;
    }

    public void logIndexItem() {
        logger.info("hashCode:{}, topicId:{}, queueId:{}, offset:{}, size:{}, timeDiff:{}", hashCode, topicId, queueId, offset, size, timeDiff);
    }
}

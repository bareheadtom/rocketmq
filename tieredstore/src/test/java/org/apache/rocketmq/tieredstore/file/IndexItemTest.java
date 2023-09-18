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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IndexItemTest {

    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    IndexItem indexItem;

    @Before
    public void setUp() {

    }

    @Test
    public void loadFromByteBuffer() {
        //init ByteBuffer
        ByteBuffer byteBuffer = ByteBuffer.allocate(IndexItem.INDEX_ITEM_SIZE);
        byteBuffer.putInt(0, 1);
        byteBuffer.putInt(4, 2);
        byteBuffer.putInt(8, 3);
        byteBuffer.putInt(16, 4);
        byteBuffer.putInt(20, 5);
        byteBuffer.putInt(24, 6);

        //test load from ByteByffer
        indexItem = new IndexItem();
        indexItem.loadFromByteBuffer(byteBuffer, 0);
        Assert.assertEquals(1, indexItem.getHashCode());
        Assert.assertEquals(2, indexItem.getTopicId());
        Assert.assertEquals(3, indexItem.getQueueId());
        Assert.assertEquals(4, indexItem.getOffset());
        Assert.assertEquals(5, indexItem.getSize());
        Assert.assertEquals(6, indexItem.getTimeDiff());

    }

    @Test
    public void updateToByteBuffer() {

        //init IndexItem
        indexItem = new IndexItem(1, 2, 3, 4, 5, 6);

        //test update ByteBuffer
        ByteBuffer byteBuffer = ByteBuffer.allocate(IndexItem.INDEX_ITEM_SIZE);
        indexItem.updateToByteBuffer(byteBuffer, 0);
        Assert.assertEquals(1, byteBuffer.getInt(0));
        Assert.assertEquals(2, byteBuffer.getInt(4));
        Assert.assertEquals(3, byteBuffer.getInt(8));
        Assert.assertEquals(4, byteBuffer.getInt(16));
        Assert.assertEquals(5, byteBuffer.getInt(20));
        Assert.assertEquals(6, byteBuffer.getInt(24));
    }
}
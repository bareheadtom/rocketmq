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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TieredIndexHeaderTest {

    TieredIndexHeader tieredIndexHeader;

    @Before
    public void setUp() {

    }

    @Test
    public void initOrRecoverFromByteBuffer() {

    }

    @Test
    public void loadFromByteBuffer() {
        //init ByteBuffer
        ByteBuffer byteBuffer = ByteBuffer.allocate(TieredIndexHeader.INDEX_HEADER_SIZE);
        byteBuffer.putInt(0, 2);
        byteBuffer.putLong(4, 0);
        byteBuffer.putLong(12, 99);
        byteBuffer.putInt(20, 2);
        byteBuffer.putInt(24, 2);

        //test load from ByteBuffer
        tieredIndexHeader = new TieredIndexHeader();
        tieredIndexHeader.loadFromByteBuffer(byteBuffer);
        Assert.assertEquals(2, tieredIndexHeader.getMagicCode());
        Assert.assertEquals(0, tieredIndexHeader.getBeginTimestamp());
        Assert.assertEquals(99, tieredIndexHeader.getEndTimestamp());
        Assert.assertEquals(2, tieredIndexHeader.getHashSlotCount());
        Assert.assertEquals(2, tieredIndexHeader.getIndexCount());

    }

    @Test
    public void updateByteBufferHeader() {
        //init IndexHeader
        tieredIndexHeader = new TieredIndexHeader();
        tieredIndexHeader.setBeginTimestamp(111);
        tieredIndexHeader.setEndTimestamp(999);

        //test update ByteBuffer
        ByteBuffer byteBuffer = ByteBuffer.allocate(TieredIndexHeader.INDEX_HEADER_SIZE);
        tieredIndexHeader.updateByteBufferHeader(byteBuffer);
        Assert.assertEquals(TieredIndexHeader.BEGIN_MAGIC_CODE, byteBuffer.getInt(0));
        Assert.assertEquals(111, byteBuffer.getLong(4));
        Assert.assertEquals(999, byteBuffer.getLong(12));
        Assert.assertEquals(0, byteBuffer.getInt(20));
        Assert.assertEquals(1, byteBuffer.getInt(24));

    }
}
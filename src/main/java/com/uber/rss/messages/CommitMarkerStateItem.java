/*
 * Copyright (c) 2020 Uber Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.rss.messages;

import com.uber.rss.clients.ShuffleWriteConfig;
import com.uber.rss.common.AppShuffleId;
import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class CommitMarkerStateItem extends BaseMessage {
    private final long timestamp;

    public CommitMarkerStateItem(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_CommitMarkerStateItem;
    }

    @Override
    public void serialize(ByteBuf buf) {
        buf.writeLong(timestamp);
    }

    public static CommitMarkerStateItem deserialize(ByteBuf buf) {
        long timestamp = buf.readLong();
        return new CommitMarkerStateItem(timestamp);
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "CommitMarkerStateItem{" +
            "timestamp=" + timestamp +
            '}';
    }
}

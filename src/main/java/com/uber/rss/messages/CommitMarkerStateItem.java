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

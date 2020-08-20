package com.uber.rss.messages;

import io.netty.buffer.ByteBuf;

/***
 * Message sent by upload client to indicate the uploading is finished.
 * TODO remove FinishUploadRequest/Response later
 */
public class FinishUploadMessage extends BaseMessage {
    private long taskAttemptId;
    private long timestamp;

    public FinishUploadMessage(long taskAttemptId, long timestamp) {
        this.taskAttemptId = taskAttemptId;
        this.timestamp = timestamp;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_FinishUploadMessage;
    }

    @Override
    public void serialize(ByteBuf buf) {
        buf.writeLong(taskAttemptId);
        buf.writeLong(timestamp);
    }

    public static FinishUploadMessage deserialize(ByteBuf buf) {
        long taskAttemptId = buf.readLong();
        long timestamp = buf.readLong();
        return new FinishUploadMessage(taskAttemptId, timestamp);
    }

    public long getTaskAttemptId() {
        return taskAttemptId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "FinishUploadMessage{" +
                "taskAttemptId=" + taskAttemptId +
                "timestamp=" + timestamp +
                '}';
    }
}

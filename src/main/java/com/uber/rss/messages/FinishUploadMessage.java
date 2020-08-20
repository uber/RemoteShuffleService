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

package com.uber.rss.clients;

import java.nio.ByteBuffer;

// key/value could be null
public class KeyValueRecord {

  private final long taskAttemptId;
  private final ByteBuffer keyBuffer;
  private final ByteBuffer valueBuffer;

  public KeyValueRecord(long taskAttemptId, ByteBuffer keyBuffer, ByteBuffer valueBuffer) {
    this.taskAttemptId = taskAttemptId;
    this.keyBuffer = keyBuffer;
    this.valueBuffer = valueBuffer;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public ByteBuffer getKeyBuffer() {
    return keyBuffer;
  }

  public ByteBuffer getValueBuffer() {
    return valueBuffer;
  }

  @Override
  public String toString() {
    return "KeyValueRecord{" +
        "taskAttemptId=" + taskAttemptId +
        ", keyBuffer=" + keyBuffer +
        ", valueBuffer=" + valueBuffer +
        '}';
  }
}

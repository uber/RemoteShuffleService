package com.uber.rss.common;

import com.uber.rss.util.ByteBufUtils;

public class DataBlockHeader {
  public static int NUM_BYTES = Long.BYTES + Integer.BYTES;

  public static byte[] serializeToBytes(long taskAttemptId, int length) {
    byte[] bytes = new byte[NUM_BYTES];
    ByteBufUtils.writeLong(bytes, 0, taskAttemptId);
    ByteBufUtils.writeInt(bytes, Long.BYTES, length);
    return bytes;
  }

  public static DataBlockHeader deserializeFromBytes(byte[] bytes) {
    long taskAttemptId = ByteBufUtils.readLong(bytes, 0);
    int length = ByteBufUtils.readInt(bytes, Long.BYTES);
    return new DataBlockHeader(taskAttemptId, length);
  }

  private final long taskAttemptId;
  private final int length;

  public DataBlockHeader(long taskAttemptId, int length) {
    this.taskAttemptId = taskAttemptId;
    this.length = length;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public int getLength() {
    return length;
  }

  @Override
  public String toString() {
    return "DataBlockHeader{" +
        "taskAttemptId=" + taskAttemptId +
        ", length=" + length +
        '}';
  }
}

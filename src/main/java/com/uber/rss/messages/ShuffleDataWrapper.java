package com.uber.rss.messages;

/***
 * This class wraps a chunk of data inside the shuffle file. The data (bytes) should
 * be written to shuffle file directly.
 */
public class ShuffleDataWrapper {
  private final int partitionId;
  private final long taskAttemptId;
  private final byte[] bytes;

  public ShuffleDataWrapper(int partitionId, long taskAttemptId, byte[] bytes) {
    if (bytes == null) {
      throw new NullPointerException("bytes");
    }

    this.partitionId = partitionId;
    this.taskAttemptId = taskAttemptId;
    this.bytes = bytes;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public byte[] getBytes() {
    return bytes;
  }

  @Override
  public String toString() {
    return "ShuffleDataWrapper{" +
        "partitionId=" + partitionId +
        ", taskAttemptId=" + taskAttemptId +
        ", bytes.length=" + bytes.length +
        '}';
  }
}

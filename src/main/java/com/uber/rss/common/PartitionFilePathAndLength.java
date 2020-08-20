package com.uber.rss.common;

import java.util.Objects;

public class PartitionFilePathAndLength {
  private int partition;
  private String path;
  private long length;

  public PartitionFilePathAndLength(int partition, String path, long length) {
    this.partition = partition;
    this.path = path;
    this.length = length;
  }

  public int getPartition() {
    return partition;
  }

  public String getPath() {
    return path;
  }

  public long getLength() {
    return length;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PartitionFilePathAndLength that = (PartitionFilePathAndLength) o;
    return partition == that.partition &&
        length == that.length &&
        Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partition, path, length);
  }

  @Override
  public String toString() {
    return "PartitionFilePathAndLength{" +
        "partition=" + partition +
        ", path='" + path + '\'' +
        ", length=" + length +
        '}';
  }
}

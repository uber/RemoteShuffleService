package com.uber.rss.clients;

import java.util.Objects;

public class ShuffleWriteConfig {

  private final String fileCompressionCodec;
  private final short numSplits;

  public ShuffleWriteConfig() {
    this("", (short)1);
  }

  public ShuffleWriteConfig(String fileCompressionCodec, short numSplits) {
    this.fileCompressionCodec = fileCompressionCodec;
    this.numSplits = numSplits;
  }

  public String getFileCompressionCodec() {
    return fileCompressionCodec;
  }

  public short getNumSplits() {
    return numSplits;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ShuffleWriteConfig that = (ShuffleWriteConfig) o;
    return numSplits == that.numSplits &&
        Objects.equals(fileCompressionCodec, that.fileCompressionCodec);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileCompressionCodec, numSplits);
  }

  @Override
  public String toString() {
    return "ShuffleWriteConfig{" +
        "fileCompressionCodec='" + fileCompressionCodec + '\'' +
        ", numSplits=" + numSplits +
        '}';
  }
}

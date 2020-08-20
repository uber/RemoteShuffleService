package com.uber.rss.common;

public class DataBlock {
  private DataBlockHeader header;
  private byte[] payload;

  public DataBlock(DataBlockHeader header, byte[] payload) {
    this.header = header;
    this.payload = payload;
  }

  public DataBlockHeader getHeader() {
    return header;
  }

  public byte[] getPayload() {
    return payload;
  }

  @Override
  public String toString() {
    return "DataBlock{" +
        "header=" + header +
        '}';
  }
}

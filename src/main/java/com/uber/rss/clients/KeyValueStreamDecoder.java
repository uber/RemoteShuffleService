package com.uber.rss.clients;

/***
 * This interface decode key value pairs.
 */
public interface KeyValueStreamDecoder {
  long readableBytes();

  void addBytes(byte[] bytes);

  KeyValueDecodeResult decode();

  int getBufferSize();

  boolean isEmpty();
}

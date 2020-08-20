package com.uber.rss.clients;

import java.nio.ByteBuffer;

// key/value could be null
public class KeyValueDecodeResult {

  private final ByteBuffer keyBuffer;
  private final ByteBuffer valueBuffer;

  public KeyValueDecodeResult(ByteBuffer keyBuffer, ByteBuffer valueBuffer) {
    this.keyBuffer = keyBuffer;
    this.valueBuffer = valueBuffer;
  }

  public ByteBuffer getKeyBuffer() {
    return keyBuffer;
  }

  public ByteBuffer getValueBuffer() {
    return valueBuffer;
  }

  @Override
  public String toString() {
    return "KeyValueDecodeResult{" +
        "keyBuffer=" + keyBuffer +
        ", valueBuffer=" + valueBuffer +
        '}';
  }
}

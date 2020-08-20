package com.uber.rss.clients;

/***
 * This class decodes compressed stream (compressed length + uncompressed length + compressed bytes),
 * where compressed bytes contains key/value stream.
 */
public class CompressedKeyValueStreamDecoder implements KeyValueStreamDecoder {

  private final CompressedStreamDecoder compressedStreamDecoder = new CompressedStreamDecoder();
  private final KeyValueStreamDecoder keyValueStreamDecoder = new PlainKeyValueStreamDecoder();

  @Override
  public long readableBytes() {
    return compressedStreamDecoder.readableBytes() + keyValueStreamDecoder.readableBytes();
  }

  @Override
  public void addBytes(byte[] bytes) {
    compressedStreamDecoder.addBytes(bytes);
  }

  @Override
  public KeyValueDecodeResult decode() {
    if (keyValueStreamDecoder.readableBytes() > 0) {
      KeyValueDecodeResult keyValueDecodeResult = keyValueStreamDecoder.decode();
      if (keyValueDecodeResult != null) {
        return keyValueDecodeResult;
      }
    }

    while (compressedStreamDecoder.readableBytes() > 0) {
      byte[] uncompressedBytes = compressedStreamDecoder.decode();
      if (uncompressedBytes == null) {
        return null;
      }
      if (uncompressedBytes.length == 0) {
        continue;
      }
      keyValueStreamDecoder.addBytes(uncompressedBytes);
      KeyValueDecodeResult keyValueDecodeResult = keyValueStreamDecoder.decode();
      if (keyValueDecodeResult != null) {
        return keyValueDecodeResult;
      }
    }
    return null;
  }

  @Override
  public int getBufferSize() {
    return compressedStreamDecoder.getBufferSize() + keyValueStreamDecoder.getBufferSize();
  }

  @Override
  public boolean isEmpty() {
    return compressedStreamDecoder.isEmpty() && keyValueStreamDecoder.isEmpty();
  }
}

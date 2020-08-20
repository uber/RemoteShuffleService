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

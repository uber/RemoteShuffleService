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

import com.uber.rss.exceptions.RssInvalidDataException;
import com.uber.rss.exceptions.RssInvalidStateException;
import com.uber.rss.exceptions.RssException;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/***
 * This class decodes compressed stream (compressed length + uncompressed length + compressed bytes).
 */
public class CompressedStreamDecoder extends StreamDecoderBase<CompressedStreamDecoder.DecodeState, byte[]> {
  private static final Logger logger = LoggerFactory.getLogger(CompressedStreamDecoder.class);

  private final LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();

  public enum DecodeState {
    READ_COMPRESSED_LEN,
    READ_UNCOMPRESSED_LEN,
    READ_BYTES
  }

  private ByteBuffer compressedBytes;
  private int compressedLen;
  private int uncompressedLen;

  public CompressedStreamDecoder() {
    super(DecodeState.READ_COMPRESSED_LEN);
  }

  public int getBufferSize() {
    if (compressedBytes == null) {
      return 0;
    }
    return compressedBytes.capacity();
  }

  public boolean isEmpty() {
    return readableBytes() == 0 && state == DecodeState.READ_COMPRESSED_LEN;
  }

  @Override
  protected byte[] decodeSingleRound() {
    if (readableBytes() <= 0) {
      throw new RssInvalidStateException(String.format("Cannot decode due to lack of readable bytes: %s", readableBytes()));
    }

    switch (state) {
      case READ_COMPRESSED_LEN:
        if (readableBytes() < Integer.BYTES) {
          return null;
        }

        compressedLen = readInt();
        if (compressedLen < 0) {
          throw new RssInvalidDataException("Compressed length should not be less than 0: " + compressedLen);
        } else {
          // following data is: uncompressed length + compressed bytes
          compressedBytes = ByteBuffer.allocate(Integer.BYTES + compressedLen);
          state = DecodeState.READ_UNCOMPRESSED_LEN;
        }
        return null;
      case READ_UNCOMPRESSED_LEN:
        if (readableBytes() < Integer.BYTES) {
          return null;
        }

        uncompressedLen = readInt();
        if (uncompressedLen < 0) {
          throw new RssInvalidDataException("Uncompressed length should not be less than 0: " + uncompressedLen);
        } else {
          if (compressedLen == 0) {
            state = DecodeState.READ_COMPRESSED_LEN;
            resetData();
            return new byte[0];
          } else {
            // compressed bytes should contain uncompressed length and then bytes, so it could be decompressed properly
            compressedBytes = ByteBuffer.allocate(compressedLen);
            state = DecodeState.READ_BYTES;
          }
        }
        return null;
      case READ_BYTES:
        int remainingByteCount = compressedBytes.capacity() - compressedBytes.position();
        if (readableBytes() < remainingByteCount) {
          long count = readableBytes();
          readBytes(count, compressedBytes);
        } else {
          readBytes(remainingByteCount, compressedBytes);
          state = DecodeState.READ_COMPRESSED_LEN;
          ByteBuffer uncompressedBytes = ByteBuffer.allocate(uncompressedLen);
          compressedBytes.flip();
          decompressor.decompress(compressedBytes, uncompressedBytes);
          byte[] result = uncompressedBytes.array();
          resetData();
          return result;
        }
        return null;
      default:
        throw new RssException(String.format(
            "Should not decode in state %s", state));
    }
  }

  private void resetData() {
    compressedLen = 0;
    uncompressedLen = 0;
    compressedBytes = null;
  }
}

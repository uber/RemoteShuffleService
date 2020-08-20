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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/***
 * This class decodes key value record stream.
 */
public class PlainKeyValueStreamDecoder extends StreamDecoderBase<PlainKeyValueStreamDecoder.DecodeState, KeyValueDecodeResult> implements KeyValueStreamDecoder {
  private static final Logger logger = LoggerFactory.getLogger(PlainKeyValueStreamDecoder.class);

  public enum DecodeState {
    READ_RECORD_KEY_LEN,
    READ_RECORD_KEY_BYTES,
    READ_RECORD_VALUE_LEN,
    READ_RECORD_VALUE_BYTES
  }

  private ByteBuffer keyBuffer;
  private ByteBuffer valueBuffer;

  public PlainKeyValueStreamDecoder() {
    super(DecodeState.READ_RECORD_KEY_LEN);
  }

  @Override
  protected KeyValueDecodeResult decodeSingleRound() {
    if (readableBytes() <= 0) {
      throw new RssInvalidStateException(String.format("Cannot decode due to lack of readable bytes: %s", readableBytes()));
    }

    switch (state) {
      case READ_RECORD_KEY_LEN:
        if (readableBytes() < Integer.BYTES) {
          return null;
        }

        int keyLen = readInt();
        if (keyLen < -1) {
          throw new RssInvalidDataException("Key length should not be less than -1: " + keyLen);
        } else if (keyLen == -1) {
          keyBuffer = null;
          state = DecodeState.READ_RECORD_VALUE_LEN;
        } else if (keyLen == 0) {
          keyBuffer = ByteBuffer.allocate(0);
          state = DecodeState.READ_RECORD_VALUE_LEN;
        } else {
          keyBuffer = ByteBuffer.allocate(keyLen);
          state = DecodeState.READ_RECORD_KEY_BYTES;
        }
        return null;
      case READ_RECORD_KEY_BYTES:
        int remainingKeyByteCount = keyBuffer.capacity() - keyBuffer.position();
        if (readableBytes() < remainingKeyByteCount) {
          long count = readableBytes();
          readBytes(count, keyBuffer);
        } else {
          readBytes(remainingKeyByteCount, keyBuffer);
          state = DecodeState.READ_RECORD_VALUE_LEN;
        }
        return null;
      case READ_RECORD_VALUE_LEN:
        if (readableBytes() < Integer.BYTES) {
          return null;
        }
        int valueLen = readInt();
        if (valueLen < -1) {
          throw new RssInvalidDataException("Value length should not be less than -1: " + valueLen);
        } else if (valueLen == -1) {
          state = DecodeState.READ_RECORD_KEY_LEN;
          KeyValueDecodeResult result = new KeyValueDecodeResult(keyBuffer, null);
          keyBuffer = null;
          valueBuffer = null;
          return result;
        } else if (valueLen == 0) {
          valueBuffer = ByteBuffer.allocate(0);
          state = DecodeState.READ_RECORD_KEY_LEN;
          KeyValueDecodeResult result = new KeyValueDecodeResult(keyBuffer, valueBuffer);
          keyBuffer = null;
          valueBuffer = null;
          return result;
        } else {
          valueBuffer = ByteBuffer.allocate(valueLen);
          state = DecodeState.READ_RECORD_VALUE_BYTES;
        }
        return null;
      case READ_RECORD_VALUE_BYTES:
        int remainingValueByteCount = valueBuffer.capacity() - valueBuffer.position();
        if (readableBytes() < remainingValueByteCount) {
          long count = readableBytes();
          readBytes(count, valueBuffer);
        } else {
          readBytes(remainingValueByteCount, valueBuffer);
          state = DecodeState.READ_RECORD_KEY_LEN;
          KeyValueDecodeResult result = new KeyValueDecodeResult(keyBuffer, valueBuffer);
          keyBuffer = null;
          valueBuffer = null;
          return result;
        }
        return null;
      default:
        throw new RssException(String.format(
            "Should not decode in state %s", state));
    }
  }

  @Override
  public int getBufferSize() {
    int keyBufferSize = keyBuffer == null ? 0 : keyBuffer.capacity();
    int valueBufferSize = valueBuffer == null ? 0 : valueBuffer.capacity();
    return keyBufferSize + valueBufferSize;
  }

  @Override
  public boolean isEmpty() {
    return readableBytes() == 0 && state == DecodeState.READ_RECORD_KEY_LEN;
  }
}

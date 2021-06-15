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

import java.nio.ByteBuffer;
import java.util.LinkedList;

public abstract class StreamDecoderBase<TState, TDecodeResult> {
    protected final LinkedList<byte[]> list = new LinkedList<>();

    protected TState state;

    protected long totalBytes = 0;

    protected int currentReadCursor = 0;

    protected long totalReadBytes = 0;

    protected StreamDecoderBase(TState initialState) {
        this.state = initialState;
    }

    protected abstract TDecodeResult decodeSingleRound();

    public TDecodeResult decode() {
        if (readableBytes() <= 0) {
            throw new RssInvalidStateException(String.format("Cannot decode due to lack of readable bytes: %s", readableBytes()));
        }

        TDecodeResult decodeResult = null;
        do {
            TState oldState = state;
            long oldReadableBytes = readableBytes();
            decodeResult = decodeSingleRound();
            if (decodeResult != null) {
                return decodeResult;
            }
            if (state.equals(oldState) && readableBytes() == oldReadableBytes) {
                return null;
            }
            if (readableBytes() == 0) {
                return null;
            }
        } while (decodeResult == null);

        return decodeResult;
    }

    public void addBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return;
        }

        list.add(bytes);
        totalBytes += bytes.length;
    }

    public long readableBytes() {
        if (totalReadBytes > totalBytes) {
            throw new RssInvalidStateException(String.format(
                    "Total read bytes (%s) larger than total bytes (%s), something is wrong",
                    totalReadBytes, totalBytes));
        } else if (totalReadBytes == totalBytes) {
            if (list.size() > 1) {
                throw new RssInvalidStateException(String.format(
                        "Total read bytes same as total bytes, but there is still %s pending byte arrays, something is wrong",
                        list.size()));
            } else if (list.size() == 1) {
                if (currentReadCursor != list.peek().length) {
                    throw new RssInvalidStateException(String.format(
                            "Total read bytes same as total bytes, but current read cursor (%s) is not at end of byte array (size: %s), something is wrong",
                            currentReadCursor, list.peek().length));
                }
                checkAndAdjustReadCursor();
            }
        }
        return totalBytes - totalReadBytes;
    }

    public byte readByte() {
        byte value;

        if (currentReadCursor <= list.peek().length - 1) {
            totalReadBytes++;
            value = list.peek()[currentReadCursor++];
        } else {
            list.pop();
            currentReadCursor = 0;

            totalReadBytes++;
            value = list.peek()[currentReadCursor++];
        }

        checkAndAdjustReadCursor();

        return value;
    }

    public void readBytes(long count, ByteBuffer byteBuffer) {
        if (count > Integer.MAX_VALUE) {
            throw new RssInvalidDataException(String.format("Trying to read too large data size %s", count));
        }
        int countIntValue = (int)count;
        while (countIntValue > 0) {
            int batchSize = Math.min(countIntValue, list.peek().length - currentReadCursor);
            byteBuffer.put(list.peek(), currentReadCursor, batchSize);
            countIntValue -= batchSize;
            totalReadBytes += batchSize;
            currentReadCursor += batchSize;
            checkAndAdjustReadCursor();
        }
    }

    public int readInt() {
        return (readByte() & 0xff) << 24 |
                (readByte() & 0xff) << 16 |
                (readByte() & 0xff) <<  8 |
                readByte() & 0xff;
    }

    public long readLong() {
        return ((long) readByte() & 0xff) << 56 |
                ((long) readByte() & 0xff) << 48 |
                ((long) readByte() & 0xff) << 40 |
                ((long) readByte() & 0xff) << 32 |
                ((long) readByte() & 0xff) << 24 |
                ((long) readByte() & 0xff) << 16 |
                ((long) readByte() & 0xff) <<  8 |
                (long) readByte() & 0xff;
    }

    private void checkAndAdjustReadCursor() {
        if (currentReadCursor == list.peek().length) {
            list.pop();
            currentReadCursor = 0;
        } else if (currentReadCursor > list.peek().length) {
            throw new RssInvalidStateException(String.format("Read cursor %s should not exceed max length %s", currentReadCursor, list.peek().length));
        }
    }
}
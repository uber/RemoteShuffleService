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

import com.uber.rss.metrics.M3Stats;
import com.uber.rss.metrics.WriteClientMetrics;
import com.uber.rss.metrics.WriteClientMetricsKey;
import io.netty.buffer.Unpooled;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/***
 * Shuffle write client to upload data (compresses records) to shuffle server.
 */
public class CompressedRecordSyncWriteClient extends RecordSyncWriteClientBase {
  private static final Logger logger =
      LoggerFactory.getLogger(CompressedRecordSyncWriteClient.class);

  private final int compressBufferSize;
  private final Map<Integer, PartitionCompressBuffer> partitionCompressBuffers = new HashMap<>();

  private WriteClientMetrics metrics = null;

  public CompressedRecordSyncWriteClient(String host, int port, int timeoutMillis, String user, String appId, String appAttempt, int compressBufferSize, ShuffleWriteConfig shuffleWriteConfig) {
    this(host, port, timeoutMillis, true, user, appId, appAttempt, compressBufferSize, shuffleWriteConfig);
  }

  public CompressedRecordSyncWriteClient(String host, int port, int timeoutMillis, boolean finishUploadAck, String user, String appId, String appAttempt, int compressBufferSize, ShuffleWriteConfig shuffleWriteConfig) {
    super(host, port, timeoutMillis, finishUploadAck, user, appId, appAttempt, shuffleWriteConfig);
    this.compressBufferSize = compressBufferSize;
    this.metrics = new WriteClientMetrics(new WriteClientMetricsKey(
        this.getClass().getSimpleName(), user));
  }

  @Override
  public void sendRecord(int partition, ByteBuffer key, ByteBuffer value) {
    int totalRecordBytes = getRecordSerializedSize(key, value);

    PartitionCompressBuffer partitionCompressBuffer = getPartitionCompressBuffer(partition);
    ByteBuffer bufferToCompress = partitionCompressBuffer.bufferToCompress;
    if (bufferToCompress.remaining() >= totalRecordBytes) {
      // put the new record to bufferToCompress, if bufferToCompress has enough space
      writeRecordToBuffer(bufferToCompress, key, value);
    } else {
      // send out current bufferToCompress if it has data
      if (bufferToCompress.position() > 0) {
        bufferToCompress.flip();
        compressBufferAndSendOut(bufferToCompress, partitionCompressBuffer.compressor, partition);
        bufferToCompress.clear();
      }
      // handle the new record
      if (totalRecordBytes >= bufferToCompress.capacity()) {
        // compress and send out the new record as a single data block
        ByteBuffer singleRecordBuffer = ByteBuffer.allocate(totalRecordBytes);
        writeRecordToBuffer(singleRecordBuffer, key, value);
        singleRecordBuffer.flip();
        compressBufferAndSendOut(singleRecordBuffer, partitionCompressBuffer.compressor, partition);
      } else {
        // put the new record to bufferToCompress
        writeRecordToBuffer(bufferToCompress, key, value);
      }
    }
  }

  @Override
  public void finishUpload() {
    for (Map.Entry<Integer, PartitionCompressBuffer> entry: partitionCompressBuffers.entrySet()) {
      int partition = entry.getKey();
      PartitionCompressBuffer buffer = entry.getValue();
      if (buffer.bufferToCompress.position() > 0) {
        buffer.bufferToCompress.flip();
        compressBufferAndSendOut(buffer.bufferToCompress, buffer.compressor, partition);
        buffer.bufferToCompress.clear();
      }
    }
    super.finishUpload();
  }

  @Override
  public void close() {
    super.close();
    partitionCompressBuffers.clear();

    closeMetrics();
  }

  @Override
  public String toString() {
    return "CompressedRecordSyncWriteClient{" +
        "compressBufferSize=" + compressBufferSize +
        ", " + super.toString() +
        '}';
  }

  private void writeRecordToBuffer(ByteBuffer buffer, ByteBuffer key, ByteBuffer value) {
    if (key == null) {
      buffer.putInt(-1);
    } else {
      buffer.putInt(key.remaining());
      buffer.put(key);
    }
    if (value == null) {
      buffer.putInt(-1);
    } else {
      buffer.putInt(value.remaining());
      buffer.put(value);
    }
  }

  private void compressBufferAndSendOut(ByteBuffer buffer, LZ4Compressor compressor, int partition) {
    int uncompressedByteCount = buffer.limit();
    ByteBuffer compressedBuffer = ByteBuffer.allocate(compressor.maxCompressedLength(buffer.limit()));
    compressor.compress(buffer, compressedBuffer);

    int compressedByteCount = compressedBuffer.position();
    compressedBuffer.flip();

    ByteBuffer lengthByteBuffer = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES);
    lengthByteBuffer.putInt(compressedByteCount);
    lengthByteBuffer.putInt(uncompressedByteCount);
    lengthByteBuffer.flip();

    dataBlockSyncWriteClient.writeData(partition, shuffleMapTaskAttemptId.getTaskAttemptId(), Unpooled.wrappedBuffer(lengthByteBuffer, compressedBuffer));
  }

  private PartitionCompressBuffer getPartitionCompressBuffer(int partition) {
    PartitionCompressBuffer buffer = partitionCompressBuffers.get(partition);
    if (buffer != null) {
      return buffer;
    }

    buffer = new PartitionCompressBuffer();
    partitionCompressBuffers.put(partition, buffer);
    metrics.getBufferSize().update(partitionCompressBuffers.size() * compressBufferSize);
    return buffer;
  }

  private void closeMetrics() {
    try {
      if (metrics != null) {
        metrics.close();
        metrics = null;
      }
    } catch (Throwable e) {
      M3Stats.addException(e, this.getClass().getSimpleName());
      logger.warn(String.format("Failed to close metrics: %s", this), e);
    }
  }

  private class PartitionCompressBuffer {
    private final LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
    private final ByteBuffer bufferToCompress = ByteBuffer.allocate(compressBufferSize);
  }
}

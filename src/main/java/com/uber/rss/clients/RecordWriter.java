package com.uber.rss.clients;

import com.uber.rss.common.AppTaskAttemptId;

import java.nio.ByteBuffer;

/***
 * Shuffle record writer.
 */
public interface RecordWriter extends AutoCloseable {

  void startUpload(AppTaskAttemptId appTaskAttemptId, int numMaps, int numPartitions);

  void sendRecord(int partition, ByteBuffer key, ByteBuffer value);

  void finishUpload();

  /**
   * Get write bytes since last start upload.
   * @return
   */
  long getShuffleWriteBytes();

  void close();
}

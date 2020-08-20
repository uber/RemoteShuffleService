package com.uber.rss.clients;

import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.messages.ConnectUploadResponse;

import java.nio.ByteBuffer;

/***
 * Shuffle write client to upload data (records) to shuffle server.
 */
public interface RecordSyncWriteClient extends SingleServerWriteClient {

  String getHost();

  int getPort();

  String getUser();

  String getAppId();

  String getAppAttempt();

  ConnectUploadResponse connect();

  void startUpload(AppTaskAttemptId appTaskAttemptId, int numMaps, int numPartitions);

  void sendRecord(int partition, ByteBuffer key, ByteBuffer value);

  void finishUpload();

  long getShuffleWriteBytes();
}

package com.uber.rss.clients;

public interface WriteClientFactory {
  RecordSyncWriteClient getOrCreateClient(String host, int port, int timeoutMillis, boolean finishUploadAck, String user, String appId, String appAttempt, int compressBufferSize, ShuffleWriteConfig shuffleWriteConfig);
}

package com.uber.rss.clients;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class creates unpooled write client.
 */
public class UnpooledWriteClientFactory implements WriteClientFactory {
  private static final Logger logger = LoggerFactory.getLogger(UnpooledWriteClientFactory.class);

  private final static UnpooledWriteClientFactory instance = new UnpooledWriteClientFactory();

  public static UnpooledWriteClientFactory getInstance() {
    return instance;
  }

  @Override
  public RecordSyncWriteClient getOrCreateClient(String host, int port, int timeoutMillis, boolean finishUploadAck, String user, String appId, String appAttempt, int compressBufferSize, ShuffleWriteConfig shuffleWriteConfig) {
    final RecordSyncWriteClient writeClient;
    if (compressBufferSize > 0) {
      writeClient = new CompressedRecordSyncWriteClient(
          host,
          port,
          timeoutMillis,
          finishUploadAck,
          user,
          appId,
          appAttempt,
          compressBufferSize,
          shuffleWriteConfig);
      logger.debug(String.format("Using CompressedRecordSyncWriteClient with compression buffer size %s", compressBufferSize));
    } else {
      writeClient = new PlainRecordSyncWriteClient(
          host,
          port,
          timeoutMillis,
          finishUploadAck,
          user,
          appId,
          appAttempt,
          shuffleWriteConfig);
      logger.debug(String.format("Using PlainRecordSyncWriteClient due to compression buffer size %s", compressBufferSize));
    }
    return writeClient;
  }
}

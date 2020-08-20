package com.uber.rss.clients;

import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.messages.ConnectUploadResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is a wrapper on top of another write client, so we could pool (reuse) that client.
 */
public class PooledRecordSyncWriteClient implements RecordSyncWriteClient {
  private static final Logger logger = LoggerFactory.getLogger(PooledRecordSyncWriteClient.class);

  private final static AtomicLong clientIdGenerator = new AtomicLong();

  private final long clientId = clientIdGenerator.getAndIncrement();

  private final RecordSyncWriteClient delegate;
  private final PooledWriteClientFactory writeClientFactory;

  private ConnectUploadResponse connectUploadResponse;

  private volatile boolean reusable = false;

  public PooledRecordSyncWriteClient(RecordSyncWriteClient delegate, PooledWriteClientFactory writeClientFactory) {
    this.delegate = delegate;
    this.writeClientFactory = writeClientFactory;
  }

  @Override
  public String getHost() {
    return delegate.getHost();
  }

  @Override
  public int getPort() {
    return delegate.getPort();
  }

  @Override
  public String getUser() {
    return delegate.getUser();
  }

  @Override
  public String getAppId() {
    return delegate.getAppId();
  }

  @Override
  public String getAppAttempt() {
    return delegate.getAppAttempt();
  }

  @Override
  public ConnectUploadResponse connect() {
    if (connectUploadResponse != null) {
      return connectUploadResponse;
    }

    connectUploadResponse = delegate.connect();
    reusable = true;
    return connectUploadResponse;
  }

  @Override
  public void startUpload(AppTaskAttemptId appTaskAttemptId, int numMaps, int numPartitions) {
    reusable = false;
    delegate.startUpload(appTaskAttemptId, numMaps, numPartitions);
  }

  @Override
  public void sendRecord(int partition, ByteBuffer key, ByteBuffer value) {
    try {
      delegate.sendRecord(partition, key, value);
    } catch (Throwable ex) {
      reusable = false;
      throw ex;
    }
  }

  @Override
  public void finishUpload() {
    try {
      delegate.finishUpload();
      reusable = true;
    } catch (Throwable ex) {
      reusable = false;
      throw ex;
    }
  }

  @Override
  public long getShuffleWriteBytes() {
    try {
      return delegate.getShuffleWriteBytes();
    } catch (Throwable ex) {
      reusable = false;
      throw ex;
    }
  }

  @Override
  public void close() {
    if (reusable) {
      writeClientFactory.returnClientToPool(this);
    } else {
      closeWithoutReuse();
    }
  }

  public long getClientId() {
    return clientId;
  }

  public boolean isReusable() {
    return reusable;
  }

  public void closeWithoutReuse() {
    logger.info(String.format("Closing connection %s without reuse", this));
    reusable = false;
    try {
      delegate.close();
    } catch (Exception e) {
      logger.warn(String.format("Failed to close underlying client %s", delegate), e);
    }
  }

  @Override
  public String toString() {
    return "PooledRecordSyncWriteClient{" +
        "delegate=" + delegate +
        ", clientId=" + clientId +
        ", reusable=" + reusable +
        '}';
  }
}

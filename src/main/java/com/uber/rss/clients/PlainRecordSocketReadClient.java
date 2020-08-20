package com.uber.rss.clients;

import com.uber.rss.common.AppShufflePartitionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/***
 * Shuffle read client to download data (uncompressed records) from shuffle server.
 */
public class PlainRecordSocketReadClient extends RecordSocketReadClient {
  private static final Logger logger =
      LoggerFactory.getLogger(PlainRecordSocketReadClient.class);

  public PlainRecordSocketReadClient(String host, int port, int timeoutMillis, String user, AppShufflePartitionId appShufflePartitionId, Collection<Long> latestTaskAttemptIds, long dataAvailablePollInterval, long dataAvailableWaitTime) {
    super(host, port, timeoutMillis, user, appShufflePartitionId, latestTaskAttemptIds, dataAvailablePollInterval, dataAvailableWaitTime);
  }

  @Override
  protected KeyValueStreamDecoder createKeyValueStreamDecoder() {
    return new PlainKeyValueStreamDecoder();
  }

  @Override
  public String toString() {
    return "PlainRecordSocketReadClient{" +
        super.toString() +
        '}';
  }
}

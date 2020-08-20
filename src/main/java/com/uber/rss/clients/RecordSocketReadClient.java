package com.uber.rss.clients;

import com.uber.rss.common.AppShufflePartitionId;
import com.uber.rss.common.DataBlockHeader;
import com.uber.rss.common.DownloadServerVerboseInfo;
import com.uber.rss.common.DataBlock;
import com.uber.rss.exceptions.RssInvalidDataException;
import com.uber.rss.exceptions.RssInvalidStateException;
import com.uber.rss.messages.ConnectDownloadResponse;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.metrics.ReadClientMetrics;
import com.uber.rss.metrics.ReadClientMetricsKey;
import com.uber.rss.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/***
 * Shuffle read client to download data (records) from shuffle server.
 */
public abstract class RecordSocketReadClient implements AutoCloseable, SingleServerReadClient {
  private static final Logger logger =
      LoggerFactory.getLogger(RecordSocketReadClient.class);

  private final DataBlockSocketReadClient dataBlockSocketReadClient;

  private Map<Long, KeyValueStreamDecoder> taskAttemptStreamData = new HashMap<>();

  private LinkedList<KeyValueRecord> decodedRecords = new LinkedList<>();

  private long shuffleReadBytes;

  private ReadClientMetrics metrics = null;

  protected RecordSocketReadClient(String host, int port, int timeoutMillis, String user, AppShufflePartitionId appShufflePartitionId, Collection<Long> latestTaskAttemptIds, long dataAvailablePollInterval, long dataAvailableWaitTime) {
    this.dataBlockSocketReadClient = new DataBlockSocketReadClient(host, port, timeoutMillis, user, appShufflePartitionId, latestTaskAttemptIds, dataAvailablePollInterval, dataAvailableWaitTime);
    this.metrics = new ReadClientMetrics(new ReadClientMetricsKey(this.getClass().getSimpleName(), user));
  }

  @Override
  public DownloadServerVerboseInfo connect() {
    ConnectDownloadResponse connectDownloadResponse = dataBlockSocketReadClient.connect();
    DownloadServerVerboseInfo downloadServerVerboseInfo = new DownloadServerVerboseInfo();
    downloadServerVerboseInfo.setId(connectDownloadResponse.getServerId());
    downloadServerVerboseInfo.setRunningVersion(connectDownloadResponse.getRunningVersion());
    downloadServerVerboseInfo.setMapTaskCommitStatus(connectDownloadResponse.getMapTaskCommitStatus());
    return downloadServerVerboseInfo;
  }

  @Override
  public void close() {
    try {
      dataBlockSocketReadClient.close();
    } catch (Throwable ex) {
      logger.warn(String.format("Failed to close %s", this), ex);
    }
    taskAttemptStreamData.clear();
    decodedRecords.clear();

    closeMetrics();
  }

  @Override
  public RecordKeyValuePair readRecord() {
    if (decodedRecords.isEmpty()) {
      List<KeyValueRecord> records = readDataBlockAndDecodeRecords();
      decodedRecords.addAll(records);
    }

    if (decodedRecords.isEmpty()) {
      return null;
    } else {
      KeyValueRecord record = decodedRecords.pop();
      return new RecordKeyValuePair(
          record.getKeyBuffer() == null ? null : record.getKeyBuffer().array(),
          record.getValueBuffer() == null ? null : record.getValueBuffer().array(),
          record.getTaskAttemptId());
    }
  }

  @Override
  public long getShuffleReadBytes() {
    return shuffleReadBytes;
  }

  @Override
  public String toString() {
    return "RecordSocketReadClient{" +
        "dataBlockSocketReadClient=" + dataBlockSocketReadClient +
        '}';
  }

  abstract protected KeyValueStreamDecoder createKeyValueStreamDecoder();

  private List<KeyValueRecord> readDataBlockAndDecodeRecords() {
    List<KeyValueRecord> records = new ArrayList<>();

    DataBlock dataBlock = dataBlockSocketReadClient.readDataBlock();
    while (dataBlock != null) {
      shuffleReadBytes += DataBlockHeader.NUM_BYTES + dataBlock.getPayload().length;

      long taskAttemptId = dataBlock.getHeader().getTaskAttemptId();
      KeyValueStreamDecoder keyValueStreamDecoder = getTaskAttemptDecoder(taskAttemptId);

      if (dataBlock.getPayload().length > 0) {
        keyValueStreamDecoder.addBytes(dataBlock.getPayload());

        try {
          KeyValueDecodeResult decodeResult = keyValueStreamDecoder.decode();
          while (decodeResult != null) {
            records.add(new KeyValueRecord(taskAttemptId, decodeResult.getKeyBuffer(), decodeResult.getValueBuffer()));
            if (keyValueStreamDecoder.readableBytes() == 0) {
              break;
            }
            decodeResult = keyValueStreamDecoder.decode();
          }
        } catch (Throwable ex) {
          String str = String.format(
              "Failed to decode data for task attempt %s after reading %s data blocks for %s, %s",
              taskAttemptId, dataBlockSocketReadClient.getReadBlocks(), dataBlockSocketReadClient.getAppShufflePartitionId(), ExceptionUtils.getSimpleMessage(ex));
          logger.warn(str, ex);
          throw new RssInvalidDataException(str, ex);
        }
      }

      if (!records.isEmpty()) {
        return records;
      }

      if (keyValueStreamDecoder.isEmpty()) {
        removeTaskAttemptDecoder(taskAttemptId);
      }

      dataBlock = dataBlockSocketReadClient.readDataBlock();
    }

    if (records.isEmpty()) {
      for (Map.Entry<Long, KeyValueStreamDecoder> entry : taskAttemptStreamData.entrySet()) {
        if (entry.getValue().readableBytes() > 0) {
          throw new RssInvalidStateException(String.format("Read client hit end of stream, but there is still unprocessed data for task attempt %s", entry.getKey()));
        }
      }
    }

    return records;
  }

  private KeyValueStreamDecoder getTaskAttemptDecoder(long taskAttemptId) {
    KeyValueStreamDecoder result = taskAttemptStreamData.get(taskAttemptId);
    if (result != null) {
      return result;
    }

    result = createKeyValueStreamDecoder();
    taskAttemptStreamData.put(taskAttemptId, result);

    int size = 0;
    for (KeyValueStreamDecoder entry: taskAttemptStreamData.values()) {
      size += entry.getBufferSize();
    }
    metrics.getBufferSize().update(size);

    return result;
  }

  private void removeTaskAttemptDecoder(long taskAttemptId) {
    taskAttemptStreamData.remove(taskAttemptId);

    int size = 0;
    for (KeyValueStreamDecoder entry: taskAttemptStreamData.values()) {
      size += entry.getBufferSize();
    }
    metrics.getBufferSize().update(size);
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
}

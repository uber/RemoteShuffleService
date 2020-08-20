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

import com.uber.m3.tally.Stopwatch;
import com.uber.rss.common.AppShufflePartitionId;
import com.uber.rss.common.Compression;
import com.uber.rss.common.MapTaskCommitStatus;
import com.uber.rss.common.DataBlock;
import com.uber.rss.common.DataBlockHeader;
import com.uber.rss.exceptions.RssEndOfStreamException;
import com.uber.rss.exceptions.ExceptionWrapper;
import com.uber.rss.exceptions.RssInvalidDataException;
import com.uber.rss.exceptions.RssInvalidStateException;
import com.uber.rss.exceptions.RssMissingShuffleWriteConfigException;
import com.uber.rss.exceptions.RssShuffleStageNotStartedException;
import com.uber.rss.exceptions.RssException;
import com.uber.rss.exceptions.RssShuffleCorruptedException;
import com.uber.rss.exceptions.RssShuffleDataNotAvailableException;
import com.uber.rss.messages.ConnectDownloadRequest;
import com.uber.rss.messages.MessageConstants;
import com.uber.rss.messages.ConnectDownloadResponse;
import com.uber.rss.messages.GetDataAvailabilityRequest;
import com.uber.rss.messages.GetDataAvailabilityResponse;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.metrics.ReadClientMetrics;
import com.uber.rss.metrics.ReadClientMetricsKey;
import com.uber.rss.util.ObjectWrapper;
import com.uber.rss.util.RetryUtils;
import com.uber.rss.util.StreamUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/***
 * Shuffle read client to download data (data blocks) from shuffle server.
 */
public class DataBlockSocketReadClient extends com.uber.rss.clients.ClientBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DataBlockSocketReadClient.class);

  private final String user;
  private final AppShufflePartitionId appShufflePartitionId;
  private final List<Long> latestTaskAttemptIds;
  private final long dataAvailablePollInterval;
  private final long dataAvailableWaitTime;

  private ReadClientMetrics metrics;

  private String fileCompressionCodec;
  private MapTaskCommitStatus knownMapTaskCommitStatus;
  // TODO remove knownTaskAttemptIds since it is no long needed
  private Set<Long> knownTaskAttemptIds;

  private boolean downloadStarted = false;

  private int totalReadDataBlocks = 0;

  public DataBlockSocketReadClient(String host, int port, int timeoutMillis, String user, AppShufflePartitionId appShufflePartitionId, Collection<Long> latestTaskAttemptIds, long dataAvailablePollInterval, long dataAvailableWaitTime) {
    super(host, port, timeoutMillis);
    this.user = user;
    this.appShufflePartitionId = appShufflePartitionId;
    this.latestTaskAttemptIds = new ArrayList<>(latestTaskAttemptIds);
    this.dataAvailablePollInterval = dataAvailablePollInterval;
    this.dataAvailableWaitTime = dataAvailableWaitTime;

    this.metrics = new ReadClientMetrics(new ReadClientMetricsKey(this.getClass().getSimpleName(), user));
  }

  public ConnectDownloadResponse connect() {
    Stopwatch stopwatch = metrics.getReadConnectLatency().start();
    try {
      return connectImpl();
    } finally {
      stopwatch.stop();
    }
  }

  private ConnectDownloadResponse connectImpl() {
    if (socket != null) {
      throw new RssInvalidStateException(String.format("Already connected to server, cannot connect again: %s", connectionInfo));
    }

    logger.debug(String.format("Connecting to server: %s", connectionInfo));

    connectSocket();

    write(MessageConstants.DOWNLOAD_UPLINK_MAGIC_BYTE);
    write(MessageConstants.DOWNLOAD_UPLINK_VERSION_3);

    ConnectDownloadRequest connectRequest = new ConnectDownloadRequest(user, appShufflePartitionId, latestTaskAttemptIds);


    ExceptionWrapper<RssException> exceptionWrapper = new ExceptionWrapper<>();

    boolean succeeded = RetryUtils.retryUntilTrue(dataAvailablePollInterval, dataAvailableWaitTime, () -> {
      try {
        writeControlMessageAndWaitResponseStatus(connectRequest);
      } catch (RssShuffleCorruptedException ex) {
        throw new RssShuffleCorruptedException("Shuffle data corrupted for: " + appShufflePartitionId, ex);
      } catch (RssMissingShuffleWriteConfigException | RssShuffleStageNotStartedException ex) {
        exceptionWrapper.setException(ex);
        logger.warn(String.format("Did not find data in server side, server may not run fast enough to get data from client or server hits some issue, %s", appShufflePartitionId), ex);
        return false;
      }
      return true;
    });

    if (!succeeded) {
      if (exceptionWrapper.getException() != null) {
        throw exceptionWrapper.getException();
      } else {
        throw new RssInvalidStateException(String.format("Failed to connect to server %s, %s", connectionInfo, appShufflePartitionId));
      }
    }

    ConnectDownloadResponse connectDownloadResponse = readResponseMessage(MessageConstants.MESSAGE_ConnectDownloadResponse, ConnectDownloadResponse::deserialize);

    logger.info(String.format("Connected to server: %s, response: %s", connectionInfo, connectDownloadResponse));

    fileCompressionCodec = connectDownloadResponse.getCompressionCodec();

    if (connectDownloadResponse.isDataAvailable()) {
      this.knownMapTaskCommitStatus = connectDownloadResponse.getMapTaskCommitStatus();
      if (this.knownMapTaskCommitStatus == null) {
        throw new RssInvalidDataException("MapTaskCommitStatus should not be null");
      }
      this.knownTaskAttemptIds = new HashSet<>(this.knownMapTaskCommitStatus.getTaskAttemptIds().values());

      // TODO delete knownTaskAttemptIds and following later
      if (!this.latestTaskAttemptIds.isEmpty()) {
        if (!new HashSet<>(this.latestTaskAttemptIds).equals(this.knownTaskAttemptIds)) {
          throw new RssInvalidDataException(String.format("Task attempt ids not matched"));
        }
      }
    }

    return connectDownloadResponse;
  }

  public GetDataAvailabilityResponse waitDataAvailable() {
    if (this.knownMapTaskCommitStatus != null) {
      throw new RssInvalidStateException("Data already available, should not wait again");
    }

    long startTime = System.currentTimeMillis();
    logger.info(String.format("Waiting for all mappers finished: %s, %s", appShufflePartitionId, connectionInfo));


    Stopwatch reducerWaitTimeStopwatch = metrics.getReducerWaitTime().start();
    final ObjectWrapper<GetDataAvailabilityResponse> getDataAvailabilityRetryLastResult = new ObjectWrapper<>();
    try {
      RetryUtils.retryUntilNotNull(dataAvailablePollInterval, dataAvailablePollInterval*10, dataAvailableWaitTime, () -> {
        GetDataAvailabilityResponse getDataAvailabilityResponse = getDataAvailability();
        getDataAvailabilityRetryLastResult.setObject(getDataAvailabilityResponse);
        if (getDataAvailabilityResponse.isDataAvailable()) {
          return getDataAvailabilityResponse;
        } else {
          return null;
        }
      });
    } finally {
      reducerWaitTimeStopwatch.stop();
    }

    logger.info(String.format("Finished waiting for all mappers to finish, partition: %s, duration: %s seconds",
        appShufflePartitionId, (System.currentTimeMillis() - startTime)/1000));

    GetDataAvailabilityResponse getDataAvailabilityRetryResult = getDataAvailabilityRetryLastResult.getObject();

    // Throw exception if not get the status which indicating all mappers are finished
    if (getDataAvailabilityRetryResult == null || !getDataAvailabilityRetryResult.isDataAvailable()) {
      // get task attempt ids from GetDataAvailabilityResponse and put them into the exception to help debugging
      String taskAttemptIdInfo = "";
      if (getDataAvailabilityRetryResult != null && getDataAvailabilityRetryResult.getMapTaskCommitStatus() != null) {
        MapTaskCommitStatus mapTaskCommitStatus = getDataAvailabilityRetryResult.getMapTaskCommitStatus();
        if (mapTaskCommitStatus.getTaskAttemptIds().isEmpty()) {
          taskAttemptIdInfo = "none";
        } else {
          taskAttemptIdInfo = mapTaskCommitStatus.getTaskAttemptIds().entrySet()
              .stream().map(t -> String.format("%s.%s", t.getKey(), t.getValue()))
              .collect(Collectors.joining(","));
        }
      }
      throw new RssShuffleDataNotAvailableException(String.format(
          "Not all mappers finished after trying %s:%s for %s millis, partition: %s, known committed tasks: %s",
          host, port, dataAvailableWaitTime, appShufflePartitionId, taskAttemptIdInfo));
    }

    this.knownMapTaskCommitStatus = getDataAvailabilityRetryResult.getMapTaskCommitStatus();
    if (this.knownMapTaskCommitStatus == null) {
      throw new RssInvalidDataException("MapTaskCommitStatus should not be null");
    }
    this.knownTaskAttemptIds = new HashSet<>(this.knownMapTaskCommitStatus.getTaskAttemptIds().values());

    // TODO delete knownTaskAttemptIds and following later
    if (!this.latestTaskAttemptIds.isEmpty()) {
      if (!new HashSet<>(this.latestTaskAttemptIds).equals(this.knownTaskAttemptIds)) {
        throw new RssInvalidDataException(String.format("Task attempt ids not matched"));
      }
    }

    return getDataAvailabilityRetryResult;
  }

  public DataBlock readDataBlock() {
    DataBlock dataBlock = readDataBlockNoCheckTaskAttemptId();
    while (dataBlock != null) {
      totalReadDataBlocks++;

      if (!knownTaskAttemptIds.contains(dataBlock.getHeader().getTaskAttemptId())) {
        // ignore the previous record and read next record
        dataBlock = readDataBlockNoCheckTaskAttemptId();
        metrics.getNumIgnoredBlocks().inc(1);
      } else {
        break;
      }
    }
    return dataBlock;
  }

  @Override
  public void close() {
    super.close();
    closeMetrics();
  }

  public int getReadBlocks() {
    return totalReadDataBlocks;
  }

  public AppShufflePartitionId getAppShufflePartitionId() {
    return appShufflePartitionId;
  }

  @Override
  public String toString() {
    return "DataBlockSocketReadClient{" +
        "user='" + user + '\'' +
        ", appShufflePartitionId=" + appShufflePartitionId +
        ", downloadStarted=" + downloadStarted +
        ", totalReadDataBlocks=" + totalReadDataBlocks +
        ", connectionInfo=" + connectionInfo +
        '}';
  }

  private void closeMetrics() {
    try {
      if (metrics != null) {
        metrics.close();
        metrics = null;
      }
    } catch (Throwable e) {
      M3Stats.addException(e, this.getClass().getSimpleName());
      logger.warn(String.format("Failed to close metrics: %s", connectionInfo), e);
    }
  }

  private GetDataAvailabilityResponse getDataAvailability() {
    GetDataAvailabilityRequest request = new GetDataAvailabilityRequest();
    writeControlMessageAndWaitResponseStatus(request);
    GetDataAvailabilityResponse getDataAvailabilityResponse = readResponseMessage(MessageConstants.MESSAGE_GetDataAvailabilityResponse, GetDataAvailabilityResponse::deserialize);
    return getDataAvailabilityResponse;
  }

  private DataBlock readDataBlockNoCheckTaskAttemptId() {
    if (!downloadStarted) {
      if (this.knownTaskAttemptIds == null) {
        waitDataAvailable();
      }

      startDownload();
      downloadStarted = true;
    }

    if (knownTaskAttemptIds == null) {
      throw new RssInvalidStateException(String.format("knownTaskAttemptIds is null, %s", connectionInfo));
    }

    if (knownTaskAttemptIds.isEmpty()) {
      throw new RssInvalidStateException(String.format("knownTaskAttemptIds is empty, %s", connectionInfo));
    }

    DataBlockHeader header = readDataBlockHeader(inputStream);
    if (header == null) {
      return null;
    }

    byte[] bytes = StreamUtils.readBytes(inputStream, header.getLength());
    if (bytes == null) {
      throw new RssEndOfStreamException("Failed to read data block: " + this.toString());
    }

    return new DataBlock(header, bytes);
  }

  private void startDownload() {
    InputStream decompressedStream = Compression.decompressStream(inputStream, fileCompressionCodec);
    if (decompressedStream != inputStream) {
      inputStream = decompressedStream;
      logger.info(String.format("Switched to compressing stream %s, %s", appShufflePartitionId, connectionInfo));
    }
  }

  private DataBlockHeader readDataBlockHeader(InputStream dataStream) {
    // Header consists of: long taskAttemptID + int length
    byte[] bytes = StreamUtils.readBytes(dataStream, DataBlockHeader.NUM_BYTES);
    if (bytes == null) {
      return null;
    }

    metrics.getNumReadBytes().inc(bytes.length);
    return DataBlockHeader.deserializeFromBytes(bytes);
  }

}

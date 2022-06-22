/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.rss.clients;

import com.uber.rss.common.AppShufflePartitionId;
import com.uber.rss.common.DataBlock;
import com.uber.rss.common.DataBlockHeader;
import com.uber.rss.common.DownloadServerVerboseInfo;
import com.uber.rss.messages.ConnectDownloadResponse;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.metrics.ReadClientMetrics;
import com.uber.rss.metrics.ReadClientMetricsKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/***
 * Shuffle read client to download data from shuffle server.
 */
public class ShuffleDataSocketReadClient implements AutoCloseable, SingleServerReadClient {
  private static final Logger logger =
      LoggerFactory.getLogger(ShuffleDataSocketReadClient.class);

  private final DataBlockSocketReadClient dataBlockSocketReadClient;

  private long shuffleReadBytes;

  private ReadClientMetrics metrics = null;

  protected ShuffleDataSocketReadClient(String host, int port, int timeoutMillis, String user,
                                        AppShufflePartitionId appShufflePartitionId,
                                        Collection<Long> fetchTaskAttemptIds,
                                        long dataAvailablePollInterval,
                                        long dataAvailableWaitTime) {
    this.dataBlockSocketReadClient =
        new DataBlockSocketReadClient(host, port, timeoutMillis, user, appShufflePartitionId,
            fetchTaskAttemptIds, dataAvailablePollInterval, dataAvailableWaitTime);
    this.metrics =
        new ReadClientMetrics(new ReadClientMetricsKey(this.getClass().getSimpleName(), user));
  }

  @Override
  public DownloadServerVerboseInfo connect() {
    try {
      ConnectDownloadResponse connectDownloadResponse = dataBlockSocketReadClient.connect();
      DownloadServerVerboseInfo downloadServerVerboseInfo = new DownloadServerVerboseInfo();
      downloadServerVerboseInfo.setId(connectDownloadResponse.getServerId());
      downloadServerVerboseInfo
          .setMapTaskCommitStatus(connectDownloadResponse.getMapTaskCommitStatus());
      return downloadServerVerboseInfo;
    } catch (RuntimeException ex) {
      logger.warn(String.format("Failed to connect %s", this), ex);
      close();
      throw ex;
    }
  }

  @Override
  public void close() {
    try {
      dataBlockSocketReadClient.close();
    } catch (Throwable ex) {
      logger.warn(String.format("Failed to close %s", this), ex);
    }

    closeMetrics();
  }

  @Override
  public TaskDataBlock readDataBlock() {
    try {
      DataBlock dataBlock = dataBlockSocketReadClient.readDataBlock();
      if (dataBlock == null) {
        return null;
      }
      shuffleReadBytes += DataBlockHeader.NUM_BYTES + dataBlock.getPayload().length;
      return new TaskDataBlock(dataBlock.getPayload(), dataBlock.getHeader().getTaskAttemptId());
    } catch (RuntimeException ex) {
      logger.warn(String.format("Failed to read shuffle data %s", this), ex);
      close();
      throw ex;
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

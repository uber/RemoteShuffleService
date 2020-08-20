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

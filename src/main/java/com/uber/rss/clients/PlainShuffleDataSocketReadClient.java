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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/***
 * Shuffle read client to download data from shuffle server.
 */
public class PlainShuffleDataSocketReadClient extends ShuffleDataSocketReadClient {
  private static final Logger logger =
      LoggerFactory.getLogger(PlainShuffleDataSocketReadClient.class);

  public PlainShuffleDataSocketReadClient(String host, int port, int timeoutMillis, String user,
                                          AppShufflePartitionId appShufflePartitionId,
                                          Collection<Long> fetchTaskAttemptIds,
                                          long dataAvailablePollInterval,
                                          long dataAvailableWaitTime) {
    super(host, port, timeoutMillis, user, appShufflePartitionId, fetchTaskAttemptIds,
        dataAvailablePollInterval, dataAvailableWaitTime);
  }

  @Override
  public String toString() {
    return "PlainRecordSocketReadClient{" +
        super.toString() +
        '}';
  }
}

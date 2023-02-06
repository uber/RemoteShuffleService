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

import com.uber.rss.exceptions.RssInvalidStateException;
import com.uber.rss.messages.FinishApplicationAttemptRequestMessage;
import com.uber.rss.messages.FinishApplicationJobRequestMessage;
import com.uber.rss.messages.MessageConstants;
import com.uber.rss.messages.ConnectNotifyRequest;
import com.uber.rss.messages.ConnectNotifyResponse;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.metrics.NotifyClientMetrics;
import com.uber.rss.metrics.NotifyClientMetricsKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Client connecting to notify server.
 */
public class NotifyClient extends com.uber.rss.clients.ClientBase {
  private static final Logger logger =
      LoggerFactory.getLogger(NotifyClient.class);

  private final String user;

  private NotifyClientMetrics metrics = null;

  public NotifyClient(String host, int port, int timeoutMillis, String user) {
    super(host, port, timeoutMillis);
    this.user = user;

    this.metrics = new NotifyClientMetrics(new NotifyClientMetricsKey(this.getClass().getSimpleName(), user));
    this.metrics.getNumClients().inc(1);
  }

  public ConnectNotifyResponse connect() {
    if (socket != null) {
      throw new RssInvalidStateException(String.format("Already connected to server, cannot connect again: %s", connectionInfo));
    }

    logger.debug("Connecting to server: {}", connectionInfo);

    connectSocket();

    write(MessageConstants.NOTIFY_UPLINK_MAGIC_BYTE);
    write(MessageConstants.NOTIFY_UPLINK_VERSION_3);

    ConnectNotifyRequest connectRequest = new ConnectNotifyRequest(user);

    writeControlMessageAndWaitResponseStatus(connectRequest);

    ConnectNotifyResponse connectResponse = readResponseMessage(MessageConstants.MESSAGE_ConnectNotifyResponse, ConnectNotifyResponse::deserialize);

    logger.info("Connected to server: {}, response: {}", connectionInfo, connectResponse);

    return connectResponse;
  }

  public void finishApplicationJob(String appId, String appAttempt, int jobId, String jobStatus, String exceptionName, String exceptionDetail) {
    FinishApplicationJobRequestMessage request = new FinishApplicationJobRequestMessage(appId, appAttempt, jobId, jobStatus, exceptionName, exceptionDetail);

    writeControlMessageAndWaitResponseStatus(request);
  }

  public void finishApplicationAttempt(String appId, String appAttempt) {
    FinishApplicationAttemptRequestMessage request = new FinishApplicationAttemptRequestMessage(appId, appAttempt);

    writeControlMessageAndWaitResponseStatus(request);
  }

  @Override
  public void close() {
    super.close();
    closeMetrics();
  }

  private void closeMetrics() {
    try {
      if (metrics != null) {
        metrics.close();
        metrics = null;
      }
    } catch (Throwable e) {
      M3Stats.addException(e, this.getClass().getSimpleName());
      logger.warn("Failed to close metrics: {}", connectionInfo, e);
    }
  }

}

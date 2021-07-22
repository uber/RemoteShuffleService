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

import com.uber.rss.messages.HeartbeatMessage;
import com.uber.rss.messages.MessageConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Socket client to send heartbeat to server
 */
public class HeartbeatSocketClient extends ClientBase {
  private static final Logger logger =
      LoggerFactory.getLogger(HeartbeatSocketClient.class);

  private final String user;
  private final String appId;
  private final String appAttempt;
  private final boolean keepLive;

  public HeartbeatSocketClient(String host, int port, int timeoutMillis, String user, String appId, String appAttempt, boolean keepLive) {
    super(host, port, timeoutMillis);
    this.user = user;
    this.appId = appId;
    this.appAttempt = appAttempt;
    this.keepLive = keepLive;
  }

  public void sendHeartbeat() {
    if (socket == null) {
      logger.debug("Connecting to server for heartbeat: {}", connectionInfo);
      connectSocket();

      write(MessageConstants.UPLOAD_UPLINK_MAGIC_BYTE);
      write(MessageConstants.UPLOAD_UPLINK_VERSION_3);
    }

    HeartbeatMessage heartbeatMessage = new HeartbeatMessage(user, appId, appAttempt, keepLive);
    writeControlMessageNotWaitResponseStatus(heartbeatMessage);
  }

  @Override
  public void close() {
    super.close();
  }
}

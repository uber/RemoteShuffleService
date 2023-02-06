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

import com.uber.rss.messages.GetBusyStatusRequest;
import com.uber.rss.messages.GetBusyStatusResponse;
import com.uber.rss.messages.MessageConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Socket client to get server busy status.
 */
public class BusyStatusSocketClient extends ClientBase {
  private static final Logger logger =
      LoggerFactory.getLogger(BusyStatusSocketClient.class);

  private final String user;

  public BusyStatusSocketClient(String host, int port, int timeoutMillis, String user) {
    super(host, port, timeoutMillis);
    this.user = user;
  }

  public GetBusyStatusResponse getBusyStatus() {
    if (socket == null) {
      logger.debug("Connecting to server to get busy status: {}", connectionInfo);
      connectSocket();

      write(MessageConstants.UPLOAD_UPLINK_MAGIC_BYTE);
      write(MessageConstants.UPLOAD_UPLINK_VERSION_3);
    }

    GetBusyStatusRequest getBusyStatusRequest = new GetBusyStatusRequest(user);
    writeControlMessageAndWaitResponseStatus(getBusyStatusRequest);

    GetBusyStatusResponse getBusyStatusResponse = readResponseMessage(MessageConstants.MESSAGE_GetBusyStatusResponse,
                                                                        GetBusyStatusResponse::deserialize);
    return getBusyStatusResponse;
  }

  @Override
  public void close() {
    super.close();
  }
}

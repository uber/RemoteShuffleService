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

import com.uber.rss.common.ServerDetail;
import com.uber.rss.exceptions.RssInvalidStateException;
import com.uber.rss.messages.*;
import com.uber.rss.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/***
 * Client connecting to registry server.
 */
public class RegistryClient extends ClientBase {
  private static final Logger logger =
      LoggerFactory.getLogger(RegistryClient.class);

  private final String user;

  public RegistryClient(String host, int port, int timeoutMillis, String user) {
    super(host, port, timeoutMillis);
    this.user = user;
  }

  public ConnectRegistryResponse connect() {
    if (socket != null) {
      throw new RssInvalidStateException(
          String.format("Already connected to server, cannot connect again: %s", connectionInfo));
    }

    logger.debug(String.format("Connecting to server: %s", connectionInfo));

    connectSocket();

    write(MessageConstants.REGISTRY_UPLINK_MAGIC_BYTE);
    write(MessageConstants.REGISTRY_UPLINK_VERSION_3);

    ConnectRegistryRequest connectRequest = new ConnectRegistryRequest(user);

    writeControlMessageAndWaitResponseStatus(connectRequest);

    ConnectRegistryResponse connectResponse =
        readResponseMessage(MessageConstants.MESSAGE_ConnectRegistryResponse,
            ConnectRegistryResponse::deserialize);

    logger.info(
        String.format("Connected to server: %s, response: %s", connectionInfo, connectResponse));

    return connectResponse;
  }

  public void registerServer(String dataCenter, String cluster, String serverId,
                             String connectionString) {
    RegisterServerRequestMessage request =
        new RegisterServerRequestMessage(dataCenter, cluster, serverId, connectionString);

    writeControlMessageAndWaitResponseStatus(request);

    readMessageLengthAndContent(RegisterServerResponseMessage::deserialize);
  }

  public List<ServerDetail> getServers(String dataCenter, String cluster, int maxCount) {
    GetServersRequestMessage request = new GetServersRequestMessage(dataCenter, cluster, maxCount);

    writeControlMessageAndWaitResponseStatus(request);

    GetServersResponseMessage response =
        readMessageLengthAndContent(GetServersResponseMessage::deserialize);
    return response.getServers();
  }

}

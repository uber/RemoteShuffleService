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

import com.uber.rss.common.ServerDetail;
import com.uber.rss.exceptions.RssInvalidStateException;
import com.uber.rss.messages.GetServersRequestMessage;
import com.uber.rss.messages.GetServersResponseMessage;
import com.uber.rss.messages.MessageConstants;
import com.uber.rss.messages.RegisterServerRequestMessage;
import com.uber.rss.messages.RegisterServerResponseMessage;
import com.uber.rss.messages.ConnectRegistryRequest;
import com.uber.rss.messages.ConnectRegistryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/***
 * Client connecting to registry server.
 */
public class RegistryClient extends com.uber.rss.clients.ClientBase {
  private static final Logger logger =
      LoggerFactory.getLogger(RegistryClient.class);

  private final String user;

  public RegistryClient(String host, int port, int timeoutMillis, String user) {
    super(host, port, timeoutMillis);
    this.user = user;
  }

  public ConnectRegistryResponse connect() {
    if (socket != null) {
      throw new RssInvalidStateException(String.format("Already connected to server, cannot connect again: %s", connectionInfo));
    }

    logger.debug("Connecting to server: {}", connectionInfo);

    connectSocket();

    write(MessageConstants.REGISTRY_UPLINK_MAGIC_BYTE);
    write(MessageConstants.REGISTRY_UPLINK_VERSION_3);

    ConnectRegistryRequest connectRequest = new ConnectRegistryRequest(user);

    writeControlMessageAndWaitResponseStatus(connectRequest);

    ConnectRegistryResponse connectResponse = readResponseMessage(MessageConstants.MESSAGE_ConnectRegistryResponse, ConnectRegistryResponse::deserialize);

    logger.info("Connected to server: {}, response: {}", connectionInfo, connectResponse);

    return connectResponse;
  }

  public void registerServer(String dataCenter, String cluster, String serverId, String runningVersion, String connectionString) {
    RegisterServerRequestMessage request = new RegisterServerRequestMessage(dataCenter, cluster, serverId, runningVersion, connectionString);

    writeControlMessageAndWaitResponseStatus(request);

    readMessageLengthAndContent(RegisterServerResponseMessage::deserialize);
  }

  public List<ServerDetail> getServers(String dataCenter, String cluster, int maxCount) {
    GetServersRequestMessage request = new GetServersRequestMessage(dataCenter, cluster, maxCount);

    writeControlMessageAndWaitResponseStatus(request);

    GetServersResponseMessage response = readMessageLengthAndContent(GetServersResponseMessage::deserialize);
    return response.getServers();
  }

}

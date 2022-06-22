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

package com.uber.rss.messages;

import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class ConnectNotifyResponse extends ServerResponseMessage {
  private String serverId;

  public ConnectNotifyResponse(String serverId) {
    this.serverId = serverId;
  }

  @Override
  public int getMessageType() {
    return MessageConstants.MESSAGE_ConnectNotifyResponse;
  }

  @Override
  public void serialize(ByteBuf buf) {
    ByteBufUtils.writeLengthAndString(buf, serverId);
  }

  public static ConnectNotifyResponse deserialize(ByteBuf buf) {
    String serverId = ByteBufUtils.readLengthAndString(buf);
    return new ConnectNotifyResponse(serverId);
  }

  public String getServerId() {
    return serverId;
  }

  @Override
  public String toString() {
    return "ConnectNotifyResponse{" +
        "serverId='" + serverId + '\'' +
        '}';
  }
}

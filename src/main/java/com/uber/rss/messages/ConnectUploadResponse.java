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

package com.uber.rss.messages;

import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class ConnectUploadResponse extends ServerResponseMessage {
    private String serverId;
    private String serverVersion;
    private String runningVersion;

    public ConnectUploadResponse(String serverId, String serverVersion, String runningVersion) {
        this.serverId = serverId;
        this.serverVersion = serverVersion;
        this.runningVersion = runningVersion;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_ConnectUploadResponse;
    }

    @Override
    public void serialize(ByteBuf buf) {
        ByteBufUtils.writeLengthAndString(buf, serverId);
        ByteBufUtils.writeLengthAndString(buf, serverVersion);
        ByteBufUtils.writeLengthAndString(buf, runningVersion);
    }

    public static ConnectUploadResponse deserialize(ByteBuf buf) {
        String serverId = ByteBufUtils.readLengthAndString(buf);
        String serverVersion = ByteBufUtils.readLengthAndString(buf);
        String runningVersion = ByteBufUtils.readLengthAndString(buf);
        return new ConnectUploadResponse(serverId, serverVersion, runningVersion);
    }

    public String getServerId() {
        return serverId;
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public String getRunningVersion() {
        return runningVersion;
    }

    @Override
    public String toString() {
        return "ConnectUploadResponse{" +
                "serverId='" + serverId + '\'' +
                "serverVersion='" + serverVersion + '\'' +
                "runningVersion='" + runningVersion + '\'' +
                '}';
    }
}

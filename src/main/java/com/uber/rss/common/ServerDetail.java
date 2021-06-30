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

package com.uber.rss.common;

import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

import java.util.Objects;

public class ServerDetail {
    public void serialize(ByteBuf buf) {
        ByteBufUtils.writeLengthAndString(buf, serverId);
        ByteBufUtils.writeLengthAndString(buf, runningVersion);
        ByteBufUtils.writeLengthAndString(buf, connectionString);
    }

    public static ServerDetail deserialize(ByteBuf buf) {
        String serverId = ByteBufUtils.readLengthAndString(buf);
        String runningVesion = ByteBufUtils.readLengthAndString(buf);
        String connectionString = ByteBufUtils.readLengthAndString(buf);
        return new ServerDetail(serverId, runningVesion, connectionString);
    }

    private final String serverId;
    private final String runningVersion;
    private final String connectionString;

    public ServerDetail(String serverId, String runningVersion, String connectionString) {
        this.serverId = serverId;
        this.runningVersion = runningVersion;
        this.connectionString = connectionString;
    }

    public String getServerId() {
        return serverId;
    }

    public String getRunningVersion() {
        return runningVersion;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public long getRunningVersionAsNumber() {
        if (runningVersion == null || runningVersion.isEmpty()) {
            return 0;
        }
        try {
            return Long.parseLong(runningVersion);
        } catch (Throwable ex) {
            return 0;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServerDetail that = (ServerDetail) o;
        return Objects.equals(serverId, that.serverId) &&
                Objects.equals(runningVersion, that.runningVersion) &&
                Objects.equals(connectionString, that.connectionString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverId, runningVersion);
    }

    @Override
    public String toString() {
        return "Server{" +
                connectionString +
                ", " + runningVersion +
                ", " + serverId +
                '}';
    }
}

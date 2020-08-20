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

    private String serverId;
    private String runningVersion;
    private String connectionString;

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
        return "ServerDetail{" +
                "serverId='" + serverId + '\'' +
                ", runningVersion='" + runningVersion + '\'' +
                ", connectionString='" + connectionString + '\'' +
                '}';
    }
}

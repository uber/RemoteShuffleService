package com.uber.rss.messages;

import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class RegisterServerRequestMessage extends ControlMessage {
    private String dataCenter;
    private String cluster;
    private String serverId;
    private String runningVersion;
    private String connectionString;

    public RegisterServerRequestMessage(String dataCenter, String cluster, String serverId, String runningVersion, String connectionString) {
        this.dataCenter = dataCenter;
        this.cluster = cluster;
        this.serverId = serverId;
        this.runningVersion = runningVersion;
        this.connectionString = connectionString;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_RegisterServerRequest;
    }

    @Override
    public void serialize(ByteBuf buf) {
        ByteBufUtils.writeLengthAndString(buf, dataCenter);
        ByteBufUtils.writeLengthAndString(buf, cluster);
        ByteBufUtils.writeLengthAndString(buf, serverId);
        ByteBufUtils.writeLengthAndString(buf, runningVersion);
        ByteBufUtils.writeLengthAndString(buf, connectionString);
    }

    public static RegisterServerRequestMessage deserialize(ByteBuf buf) {
        String dataCenter = ByteBufUtils.readLengthAndString(buf);
        String cluster = ByteBufUtils.readLengthAndString(buf);
        String serverId = ByteBufUtils.readLengthAndString(buf);
        String runningVersion = ByteBufUtils.readLengthAndString(buf);
        String connectionString = ByteBufUtils.readLengthAndString(buf);
        return new RegisterServerRequestMessage(dataCenter, cluster, serverId, runningVersion, connectionString);
    }

    public String getDataCenter() {
        return dataCenter;
    }

    public String getCluster() {
        return cluster;
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
    public String toString() {
        return "RegisterServerRequestMessage{" +
                "dataCenter='" + dataCenter + '\'' +
                ", cluster='" + cluster + '\'' +
                ", serverId='" + serverId + '\'' +
                ", runningVersion='" + runningVersion + '\'' +
                ", connectionString='" + connectionString + '\'' +
                '}';
    }
}

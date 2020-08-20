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

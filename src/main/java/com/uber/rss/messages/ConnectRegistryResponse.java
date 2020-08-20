package com.uber.rss.messages;

import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class ConnectRegistryResponse extends ServerResponseMessage {
    private String serverId;

    public ConnectRegistryResponse(String serverId) {
        this.serverId = serverId;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_ConnectRegistryResponse;
    }

    @Override
    public void serialize(ByteBuf buf) {
        ByteBufUtils.writeLengthAndString(buf, serverId);
    }

    public static ConnectRegistryResponse deserialize(ByteBuf buf) {
        String serverId = ByteBufUtils.readLengthAndString(buf);
        return new ConnectRegistryResponse(serverId);
    }

    public String getServerId() {
        return serverId;
    }

    @Override
    public String toString() {
        return "ConnectRegistryResponse{" +
                "serverId='" + serverId + '\'' +
                '}';
    }
}

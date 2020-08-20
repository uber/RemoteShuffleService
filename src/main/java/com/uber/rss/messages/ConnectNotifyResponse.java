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

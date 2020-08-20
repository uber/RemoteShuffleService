package com.uber.rss.messages;

import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class RegisterServerResponseMessage extends ServerResponseMessage {
    private String serverId;

    public RegisterServerResponseMessage(String serverId) {
        this.serverId = serverId;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_RegisterServerResponse;
    }

    @Override
    public void serialize(ByteBuf buf) {
        ByteBufUtils.writeLengthAndString(buf, serverId);
    }

    public static RegisterServerResponseMessage deserialize(ByteBuf buf) {
        String serverId = ByteBufUtils.readLengthAndString(buf);
        return new RegisterServerResponseMessage(serverId);
    }

    public String getServerId() {
        return serverId;
    }

    @Override
    public String toString() {
        return "RegisterServerResponseMessage{" +
                "serverId='" + serverId + '\'' +
                '}';
    }
}

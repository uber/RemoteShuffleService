package com.uber.rss.messages;

import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class ConnectRegistryRequest extends BaseMessage {
    private String user;

    public ConnectRegistryRequest(String user) {
        this.user = user;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_ConnectRegistryRequest;
    }

    @Override
    public void serialize(ByteBuf buf) {
        ByteBufUtils.writeLengthAndString(buf, user);
    }

    public static ConnectRegistryRequest deserialize(ByteBuf buf) {
        String user = ByteBufUtils.readLengthAndString(buf);
        return new ConnectRegistryRequest(user);
    }

    public String getUser() {
        return user;
    }

    @Override
    public String toString() {
        return "ConnectRegistryRequest{" +
                "user='" + user + '\'' +
                '}';
    }
}

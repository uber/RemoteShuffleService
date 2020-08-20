package com.uber.rss.messages;

import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class ConnectNotifyRequest extends BaseMessage {
    private String user;

    public ConnectNotifyRequest(String user) {
        this.user = user;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_ConnectNotifyRequest;
    }

    @Override
    public void serialize(ByteBuf buf) {
        ByteBufUtils.writeLengthAndString(buf, user);
    }

    public static ConnectNotifyRequest deserialize(ByteBuf buf) {
        String user = ByteBufUtils.readLengthAndString(buf);
        return new ConnectNotifyRequest(user);
    }

    public String getUser() {
        return user;
    }

    @Override
    public String toString() {
        return "ConnectNotifyRequest{" +
                "user='" + user + '\'' +
                '}';
    }
}

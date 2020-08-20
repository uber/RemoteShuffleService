package com.uber.rss.messages;

import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class CloseConnectionMessage extends ControlMessage {
    private String info;

    public CloseConnectionMessage(String info) {
        this.info = info;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_CloseConnectionMessage;
    }

    @Override
    public void serialize(ByteBuf buf) {
        ByteBufUtils.writeLengthAndString(buf, info);
    }

    public static CloseConnectionMessage deserialize(ByteBuf buf) {
        String info = ByteBufUtils.readLengthAndString(buf);
        return new CloseConnectionMessage(info);
    }

    public String getInfo() {
        return info;
    }

    @Override
    public String toString() {
        return "CloseConnectionMessage{" +
                "info='" + info + '\'' +
                '}';
    }
}

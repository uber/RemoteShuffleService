package com.uber.rss.messages;

import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class FinishApplicationAttemptRequestMessage extends ControlMessage {
    private String appId;
    private String appAttempt;

    public FinishApplicationAttemptRequestMessage(String appId, String appAttempt) {
        this.appId = appId;
        this.appAttempt = appAttempt;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_FinishApplicationAttemptRequest;
    }

    @Override
    public void serialize(ByteBuf buf) {
        ByteBufUtils.writeLengthAndString(buf, appId);
        ByteBufUtils.writeLengthAndString(buf, appAttempt);
    }

    public static FinishApplicationAttemptRequestMessage deserialize(ByteBuf buf) {
        String appId = ByteBufUtils.readLengthAndString(buf);
        String appAttempt = ByteBufUtils.readLengthAndString(buf);
        return new FinishApplicationAttemptRequestMessage(appId, appAttempt);
    }

    public String getAppId() {
        return appId;
    }

    public String getAppAttempt() {
        return appAttempt;
    }

    @Override
    public String toString() {
        return "FinishApplicationAttemptRequestMessage{" +
                "appId='" + appId + '\'' +
                ", appAttempt='" + appAttempt + '\'' +
                '}';
    }
}

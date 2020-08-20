package com.uber.rss.messages;

import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class ConnectUploadRequest extends BaseMessage {
    private String user;
    private String appId;
    private String appAttempt;

    public ConnectUploadRequest(String user, String appId, String appAttempt) {
        this.user = user;
        this.appId = appId;
        this.appAttempt = appAttempt;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_ConnectUploadRequest;
    }

    @Override
    public void serialize(ByteBuf buf) {
        ByteBufUtils.writeLengthAndString(buf, user);
        ByteBufUtils.writeLengthAndString(buf, appId);
        ByteBufUtils.writeLengthAndString(buf, appAttempt);
    }

    public static ConnectUploadRequest deserialize(ByteBuf buf) {
        String user = ByteBufUtils.readLengthAndString(buf);
        String appId = ByteBufUtils.readLengthAndString(buf);
        String appAttempt = ByteBufUtils.readLengthAndString(buf);
        return new ConnectUploadRequest(user, appId, appAttempt);
    }

    public String getUser() {
        return user;
    }

    public String getAppId() {
        return appId;
    }

    public String getAppAttempt() {
        return appAttempt;
    }

    @Override
    public String toString() {
        return "ConnectUploadRequest{" +
                "user='" + user + '\'' +
                ", appId='" + appId + '\'' +
                ", appAttempt='" + appAttempt + '\'' +
                '}';
    }
}

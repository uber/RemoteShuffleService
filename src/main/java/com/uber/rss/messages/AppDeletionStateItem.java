package com.uber.rss.messages;

import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class AppDeletionStateItem extends BaseMessage {
    private final String appId;

    public AppDeletionStateItem(String appId) {
        this.appId = appId;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_AppDeletionStateItem;
    }

    @Override
    public void serialize(ByteBuf buf) {
        ByteBufUtils.writeLengthAndString(buf, appId);
    }

    public static AppDeletionStateItem deserialize(ByteBuf buf) {
        String appId = ByteBufUtils.readLengthAndString(buf);
        return new AppDeletionStateItem(appId);
    }

    public String getAppId() {
        return appId;
    }
    @Override
    public String toString() {
        return "AppDeletionStateItem{" +
            "appId=" + appId +
            '}';
    }
}

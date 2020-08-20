package com.uber.rss.messages;

import com.uber.rss.common.AppShuffleId;
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class StageCorruptionStateItem extends BaseMessage {
    private final AppShuffleId appShuffleId;

    public StageCorruptionStateItem(AppShuffleId appShuffleId) {
        this.appShuffleId = appShuffleId;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_StageCorruptionStateItem;
    }

    @Override
    public void serialize(ByteBuf buf) {
        ByteBufUtils.writeLengthAndString(buf, appShuffleId.getAppId());
        ByteBufUtils.writeLengthAndString(buf, appShuffleId.getAppAttempt());
        buf.writeInt(appShuffleId.getShuffleId());
    }

    public static StageCorruptionStateItem deserialize(ByteBuf buf) {
        String appId = ByteBufUtils.readLengthAndString(buf);
        String appAttempt = ByteBufUtils.readLengthAndString(buf);
        int shuffleId = buf.readInt();
        return new StageCorruptionStateItem(new AppShuffleId(appId, appAttempt, shuffleId));
    }

    public AppShuffleId getAppShuffleId() {
        return appShuffleId;
    }

    @Override
    public String toString() {
        return "StageCorruptionStateItem{" +
            "AppShuffleId=" + appShuffleId +
            '}';
    }
}

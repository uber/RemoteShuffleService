package com.uber.rss.messages;

import com.uber.rss.clients.ShuffleWriteConfig;
import com.uber.rss.common.AppShuffleId;
import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class StageInfoStateItem extends BaseMessage {
    private final AppShuffleId appShuffleId;
    private final int numMaps;
    private final int numPartitions;
    private final int fileStartIndex;
    private final ShuffleWriteConfig writeConfig;
    private final byte fileStatus;

    public StageInfoStateItem(AppShuffleId appShuffleId, int numMaps, int numPartitions, int fileStartIndex, ShuffleWriteConfig writeConfig, byte fileStatus) {
        this.appShuffleId = appShuffleId;
        this.numMaps = numMaps;
        this.numPartitions = numPartitions;
        this.fileStartIndex = fileStartIndex;
        this.writeConfig = writeConfig;
        this.fileStatus = fileStatus;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_StageInfoStateItem;
    }

    @Override
    public void serialize(ByteBuf buf) {
        ByteBufUtils.writeLengthAndString(buf, appShuffleId.getAppId());
        ByteBufUtils.writeLengthAndString(buf, appShuffleId.getAppAttempt());
        buf.writeInt(appShuffleId.getShuffleId());
        buf.writeInt(numMaps);
        buf.writeInt(numPartitions);
        buf.writeInt(fileStartIndex);
        ByteBufUtils.writeLengthAndString(buf, writeConfig.getFileCompressionCodec());
        buf.writeShort(writeConfig.getNumSplits());
        buf.writeByte(fileStatus);
    }

    public static StageInfoStateItem deserialize(ByteBuf buf) {
        String appId = ByteBufUtils.readLengthAndString(buf);
        String appAttempt = ByteBufUtils.readLengthAndString(buf);
        int shuffleId = buf.readInt();
        int numMaps = buf.readInt();
        int numPartitions = buf.readInt();
        int fileStartIndex = buf.readInt();
        String codec = ByteBufUtils.readLengthAndString(buf);
        short numSplits = buf.readShort();
        byte fileStatus = buf.readByte();
        return new StageInfoStateItem(new AppShuffleId(appId, appAttempt, shuffleId),
            numMaps,
            numPartitions,
            fileStartIndex,
            new ShuffleWriteConfig(codec, numSplits),
            fileStatus);
    }

    public AppShuffleId getAppShuffleId() {
        return appShuffleId;
    }

    public int getNumMaps() {
        return numMaps;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public int getFileStartIndex() {
        return fileStartIndex;
    }

    public ShuffleWriteConfig getWriteConfig() {
        return writeConfig;
    }

    public byte getFileStatus() {
        return fileStatus;
    }

    @Override
    public String toString() {
        return "StageInfoStateItem{" +
            "appShuffleId=" + appShuffleId +
            ", numMaps=" + numMaps +
            ", numPartitions=" + numPartitions +
            ", fileStartIndex=" + fileStartIndex +
            ", writeConfig=" + writeConfig +
            ", fileStatus=" + fileStatus +
            '}';
    }
}

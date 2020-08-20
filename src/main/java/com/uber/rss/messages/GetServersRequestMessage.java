package com.uber.rss.messages;

import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class GetServersRequestMessage extends ControlMessage {
    private String dataCenter;
    private String cluster;
    private int maxCount;

    public GetServersRequestMessage(String dataCenter, String cluster, int maxCount) {
        this.dataCenter = dataCenter;
        this.cluster = cluster;
        this.maxCount = maxCount;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_GetServersRequest;
    }

    @Override
    public void serialize(ByteBuf buf) {
        ByteBufUtils.writeLengthAndString(buf, dataCenter);
        ByteBufUtils.writeLengthAndString(buf, cluster);
        buf.writeInt(maxCount);
    }

    public static GetServersRequestMessage deserialize(ByteBuf buf) {
        String dataCenter = ByteBufUtils.readLengthAndString(buf);
        String cluster = ByteBufUtils.readLengthAndString(buf);
        int maxCount = buf.readInt();
        return new GetServersRequestMessage(dataCenter, cluster, maxCount);
    }

    public String getDataCenter() {
        return dataCenter;
    }

    public String getCluster() {
        return cluster;
    }

    public int getMaxCount() {
        return maxCount;
    }

    @Override
    public String toString() {
        return "GetServersRequestMessage{" +
                "dataCenter='" + dataCenter + '\'' +
                ", cluster='" + cluster + '\'' +
                ", maxCount='" + maxCount + '\'' +
                '}';
    }
}

package com.uber.rss.messages;

import com.uber.rss.common.AppShuffleId;
import com.uber.rss.common.MapTaskAttemptId;
import com.uber.rss.common.PartitionFilePathAndLength;
import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class TaskAttemptCommitStateItem extends BaseMessage {
    private final AppShuffleId appShuffleId;
    private final Collection<MapTaskAttemptId> mapTaskAttemptIds;
    private final Collection<PartitionFilePathAndLength> partitionFilePathAndLengths;

    public TaskAttemptCommitStateItem(AppShuffleId appShuffleId, Collection<MapTaskAttemptId> mapTaskAttemptIds, Collection<PartitionFilePathAndLength> partitionFilePathAndLengths) {
        this.appShuffleId = appShuffleId;
        this.mapTaskAttemptIds = Collections.unmodifiableCollection(mapTaskAttemptIds);
        this.partitionFilePathAndLengths = Collections.unmodifiableCollection(partitionFilePathAndLengths);
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_TaskAttemptCommitStateItem;
    }

    @Override
    public void serialize(ByteBuf buf) {
        ByteBufUtils.writeLengthAndString(buf, appShuffleId.getAppId());
        ByteBufUtils.writeLengthAndString(buf, appShuffleId.getAppAttempt());
        buf.writeInt(appShuffleId.getShuffleId());
        buf.writeInt(mapTaskAttemptIds.size());
        for (MapTaskAttemptId entry: mapTaskAttemptIds) {
            buf.writeInt(entry.getMapId());
            buf.writeLong(entry.getTaskAttemptId());
        }
        buf.writeInt(partitionFilePathAndLengths.size());
        for (PartitionFilePathAndLength entry: partitionFilePathAndLengths) {
            buf.writeInt(entry.getPartition());
            ByteBufUtils.writeLengthAndString(buf, entry.getPath());
            buf.writeLong(entry.getLength());
        }
    }

    public static TaskAttemptCommitStateItem deserialize(ByteBuf buf) {
        String appId = ByteBufUtils.readLengthAndString(buf);
        String appAttempt = ByteBufUtils.readLengthAndString(buf);
        int shuffleId = buf.readInt();
        int count = buf.readInt();
        List<MapTaskAttemptId> mapTaskAttemptIdList = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int mapId = buf.readInt();
            long taskAttemptId = buf.readLong();
            mapTaskAttemptIdList.add(new MapTaskAttemptId(mapId, taskAttemptId));
        }
        count = buf.readInt();
        List<PartitionFilePathAndLength> partitionFilePathAndLengthList = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int partition = buf.readInt();
            String path = ByteBufUtils.readLengthAndString(buf);
            long length = buf.readLong();
            partitionFilePathAndLengthList.add(new PartitionFilePathAndLength(partition, path, length));
        }
        return new TaskAttemptCommitStateItem(new AppShuffleId(appId, appAttempt, shuffleId),
            mapTaskAttemptIdList, partitionFilePathAndLengthList);
    }

    public AppShuffleId getAppShuffleId() {
        return appShuffleId;
    }

    public Collection<MapTaskAttemptId> getMapTaskAttemptIds() {
        return mapTaskAttemptIds;
    }

    public Collection<PartitionFilePathAndLength> getPartitionFilePathAndLengths() {
        return partitionFilePathAndLengths;
    }

    @Override
    public String toString() {
        return "TaskAttemptCommitStateItem{" +
            "appShuffleId=" + appShuffleId +
            ", mapTaskAttemptIds=" + StringUtils.join(mapTaskAttemptIds, ',') +
            ", partitionFilePathAndLengths=" + StringUtils.join(partitionFilePathAndLengths, ',') +
            '}';
    }
}

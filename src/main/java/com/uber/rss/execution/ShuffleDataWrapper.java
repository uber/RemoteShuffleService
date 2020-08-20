package com.uber.rss.execution;

import com.uber.rss.common.AppShuffleId;
import io.netty.buffer.ByteBuf;

/***
 * This class wraps an operation to write a shuffle data record.
 */
public class ShuffleDataWrapper {
    private final AppShuffleId shuffleId;
    private final int mapId;
    private final long taskAttemptId;
    private final int partition;
    private final ByteBuf bytes;

    public ShuffleDataWrapper(AppShuffleId shuffleId,
                              int mapId,
                              long taskAttemptId,
                              int partition,
                              ByteBuf bytes) {
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        this.taskAttemptId = taskAttemptId;
        this.partition = partition;
        this.bytes = bytes;
    }

    public AppShuffleId getShuffleId() {
        return shuffleId;
    }

    public int getMapId() {
        return mapId;
    }

    public long getTaskAttemptId() {
        return taskAttemptId;
    }

    public int getPartition() {
        return partition;
    }

    public ByteBuf getBytes() {
        return bytes;
    }
}

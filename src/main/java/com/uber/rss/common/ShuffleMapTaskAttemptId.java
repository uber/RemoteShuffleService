package com.uber.rss.common;

import java.util.Objects;

/***
 * Application shuffle map task attempt id without app id / app attempt id
 */
public class ShuffleMapTaskAttemptId {
    private final int shuffleId;
    private final int mapId;
    private final long taskAttemptId;

    public ShuffleMapTaskAttemptId(int shuffleId, int mapId, long taskAttemptId) {
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        this.taskAttemptId = taskAttemptId;
    }

    public int getShuffleId() {
        return shuffleId;
    }

    public int getMapId() {
        return mapId;
    }

    public long getTaskAttemptId() {
        return taskAttemptId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShuffleMapTaskAttemptId that = (ShuffleMapTaskAttemptId) o;
        return shuffleId == that.shuffleId &&
            mapId == that.mapId &&
            taskAttemptId == that.taskAttemptId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shuffleId, mapId, taskAttemptId);
    }

    @Override
    public String toString() {
        return "ShuffleMapTaskAttemptId{" +
            "shuffleId=" + shuffleId +
            ", mapId=" + mapId +
            ", taskAttemptId=" + taskAttemptId +
            '}';
    }
}

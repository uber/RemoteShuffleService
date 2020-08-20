package com.uber.rss.common;

import java.util.Objects;

public class MapTaskAttemptId {
  private final int mapId;
  private final long taskAttemptId;

  public MapTaskAttemptId(int mapId, long taskAttemptId) {
    this.mapId = mapId;
    this.taskAttemptId = taskAttemptId;
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
    MapTaskAttemptId that = (MapTaskAttemptId) o;
    return mapId == that.mapId &&
        taskAttemptId == that.taskAttemptId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(mapId, taskAttemptId);
  }

  @Override
  public String toString() {
    return "MapTaskAttemptId{" +
        mapId +
        "." + taskAttemptId +
        '}';
  }
}

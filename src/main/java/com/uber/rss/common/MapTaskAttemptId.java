/*
 * Copyright (c) 2020 Uber Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

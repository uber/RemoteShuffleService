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

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class MapTaskCommitStatus {
    public void serialize(ByteBuf buf) {
        buf.writeInt(getMapperCount());
        buf.writeInt(getTaskAttemptIds().size());
        getTaskAttemptIds().forEach((mapId, taskId) -> {
            buf.writeInt(mapId);
            buf.writeLong(taskId);
        });
    }

    public static MapTaskCommitStatus deserialize(ByteBuf buf) {
        int mapperCount = buf.readInt();
        int size = buf.readInt();

        Map<Integer, Long> hashMap = new HashMap<>();
        for (int i = 0; i < size; i++) {
            int mapId = buf.readInt();
            long taskId = buf.readLong();
            hashMap.put(mapId, taskId);
        }

        return new MapTaskCommitStatus(mapperCount, hashMap);
    }

    // How many mappers in the shuffle stage
    private final int mapperCount;
    
    // Last successful attempt ids for each mapper id
    private final Map<Integer, Long> taskAttemptIds;

    public MapTaskCommitStatus(int mapperCount, Map<Integer, Long> taskAttemptIds) {
        this.mapperCount = mapperCount;
        this.taskAttemptIds = taskAttemptIds;
    }

    public int getMapperCount() {
        return mapperCount;
    }

    public Map<Integer, Long> getTaskAttemptIds() {
        return taskAttemptIds;
    }

    public boolean isPartitionDataAvailable() {
        return mapperCount != 0
            && taskAttemptIds.size() == mapperCount;
    }

    public boolean isPartitionDataAvailable(Collection<Long> knownLatestTaskAttemptIds) {
        // TODO need to verify knownLatestTaskAttemptIds non empty to make code safer
        if (knownLatestTaskAttemptIds.isEmpty()) {
            return isPartitionDataAvailable();
        }

        boolean mapperCountMatches = mapperCount != 0
            && getTaskAttemptIds().size() == mapperCount;
        if (!mapperCountMatches) {
            return false;
        }

        // TODO improve performance in following
        return taskAttemptIds.values().stream().sorted().collect(Collectors.toList())
            .equals(knownLatestTaskAttemptIds.stream().sorted().collect(Collectors.toList()));
    }

    @Override
    public String toString() {
        String str = StringUtils.join(taskAttemptIds.values(), ',');
        return "MapTaskCommitStatus{" +
                "mapperCount=" + mapperCount +
                ", taskAttemptIds=" + str +
                '}';
    }
}

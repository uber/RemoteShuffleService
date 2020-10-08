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

import com.uber.rss.exceptions.RssInvalidDataException;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

public class MapTaskCommitStatus {
    public void serialize(ByteBuf buf) {
        buf.writeInt(getTaskAttemptIds().size());
        getTaskAttemptIds().forEach((mapId, taskId) -> {
            buf.writeInt(mapId);
            buf.writeLong(taskId);
        });
    }

    public static MapTaskCommitStatus deserialize(ByteBuf buf) {
        int size = buf.readInt();

        Map<Integer, Long> hashMap = new HashMap<>();
        for (int i = 0; i < size; i++) {
            int mapId = buf.readInt();
            long taskId = buf.readLong();
            hashMap.put(mapId, taskId);
        }

        return new MapTaskCommitStatus(hashMap);
    }

    // Last successful attempt ids for each mapper id
    private final Map<Integer, Long> taskAttemptIds;

    // TODO spark 3.0 remove key in taskAttemptIds map
    public MapTaskCommitStatus(Map<Integer, Long> taskAttemptIds) {
        this.taskAttemptIds = taskAttemptIds;
    }

    public Map<Integer, Long> getTaskAttemptIds() {
        return taskAttemptIds;
    }

    public boolean isPartitionDataAvailable(Collection<Long> fetchTaskAttemptIds) {
        // TODO need to verify fetchTaskAttemptIds non empty to make code safer
        if (fetchTaskAttemptIds.isEmpty()) {
            throw new RssInvalidDataException("fetchTaskAttemptIds cannot be empty");
        }

        // TODO improve performance in following
        return new HashSet<>(taskAttemptIds.values())
            .containsAll(fetchTaskAttemptIds);
    }

    public String toShortString() {
        String str = String.format("(%s items)", taskAttemptIds.size());
        return "MapTaskCommitStatus{" +
            ", taskAttemptIds=" + str +
            '}';
    }

    @Override
    public String toString() {
        String str = StringUtils.join(taskAttemptIds.values(), ',');
        return "MapTaskCommitStatus{" +
                ", taskAttemptIds=" + str +
                '}';
    }
}

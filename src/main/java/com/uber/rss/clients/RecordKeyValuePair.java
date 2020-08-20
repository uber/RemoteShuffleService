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

package com.uber.rss.clients;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

/***
 * This class contains key and value for a shuffle record, and is used only on the client side.
 * key or value could be null.
 */
public class RecordKeyValuePair {
    private final byte[] key;
    private final byte[] value;
    
    private long taskAttemptId;

    public RecordKeyValuePair(byte[] key, byte[] value, long taskAttemptId) {
        this.key = key;
        this.value = value;
        this.taskAttemptId = taskAttemptId;
    }

    @Nullable
    public byte[] getKey() {
        return key;
    }

    @Nullable
    public byte[] getValue() {
        return value;
    }

    public long getTaskAttemptId() {
        return taskAttemptId;
    }

    public long totalBytes() {
        long bytes = 0L;
        if (key != null) {
            bytes += key.length;
        }
        if (value != null) {
            bytes += value.length;
        }
        return bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecordKeyValuePair that = (RecordKeyValuePair) o;
        return taskAttemptId == that.taskAttemptId &&
            Arrays.equals(key, that.key) &&
            Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(taskAttemptId);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        String keyStr = key == null ? "null" : key.length + " bytes";
        String valueStr = value == null ? "null" : value.length + " bytes";
        return "RecordKeyValuePair{" +
                "taskAttemptId=" + taskAttemptId +
                ", key=" + keyStr +
                ", value=" + valueStr +
                '}';
    }
}

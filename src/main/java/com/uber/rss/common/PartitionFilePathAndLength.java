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

public class PartitionFilePathAndLength {
  private int partition;
  private String path;
  private long length;

  public PartitionFilePathAndLength(int partition, String path, long length) {
    this.partition = partition;
    this.path = path;
    this.length = length;
  }

  public int getPartition() {
    return partition;
  }

  public String getPath() {
    return path;
  }

  public long getLength() {
    return length;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionFilePathAndLength that = (PartitionFilePathAndLength) o;
    return partition == that.partition &&
        length == that.length &&
        Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partition, path, length);
  }

  @Override
  public String toString() {
    return "PartitionFilePathAndLength{" +
        "partition=" + partition +
        ", path='" + path + '\'' +
        ", length=" + length +
        '}';
  }
}

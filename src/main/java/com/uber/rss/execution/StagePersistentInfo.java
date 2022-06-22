/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.rss.execution;

import com.uber.rss.clients.ShuffleWriteConfig;

public class StagePersistentInfo {
  private final int numPartitions;
  private final int fileStartIndex;

  private final ShuffleWriteConfig shuffleWriteConfig;

  private final byte fileStatus;

  public StagePersistentInfo(int numPartitions, int fileStartIndex,
                             ShuffleWriteConfig shuffleWriteConfig, byte fileStatus) {
    this.numPartitions = numPartitions;
    this.fileStartIndex = fileStartIndex;
    this.shuffleWriteConfig = shuffleWriteConfig;
    this.fileStatus = fileStatus;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public int getFileStartIndex() {
    return fileStartIndex;
  }

  public ShuffleWriteConfig getShuffleWriteConfig() {
    return shuffleWriteConfig;
  }

  public byte getFileStatus() {
    return fileStatus;
  }

  @Override
  public String toString() {
    return "StagePersistentInfo{" +
        ", numPartitions=" + numPartitions +
        ", fileStartIndex=" + fileStartIndex +
        ", shuffleWriteConfig=" + shuffleWriteConfig +
        ", fileStatus=" + fileStatus +
        '}';
  }
}

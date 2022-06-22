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

import com.uber.rss.common.AppShuffleId;
import io.netty.buffer.ByteBuf;

/***
 * This class wraps an operation to write a shuffle data record.
 */
public class ShuffleDataWrapper {
  private final AppShuffleId shuffleId;
  private final long taskAttemptId;
  private final int partition;
  private final ByteBuf bytes;

  public ShuffleDataWrapper(AppShuffleId shuffleId,
                            long taskAttemptId,
                            int partition,
                            ByteBuf bytes) {
    this.shuffleId = shuffleId;
    this.taskAttemptId = taskAttemptId;
    this.partition = partition;
    this.bytes = bytes;
  }

  public AppShuffleId getShuffleId() {
    return shuffleId;
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

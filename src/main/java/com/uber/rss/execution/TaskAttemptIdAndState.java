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

import com.uber.rss.exceptions.RssInvalidStateException;

/**
 * This class stores state for a task attempt.
 */
public class TaskAttemptIdAndState {
  private enum TaskAttemptState {
    NOT_STARTED,
    START_UPLOAD,
    COMMITTED
  }

  private long taskAttemptId;
  private TaskAttemptState state = TaskAttemptState.NOT_STARTED;

  public TaskAttemptIdAndState(long taskAttemptId) {
    this.taskAttemptId = taskAttemptId;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public void markStartUpload() {
    TaskAttemptState targetState = TaskAttemptState.START_UPLOAD;
    if (state != TaskAttemptState.NOT_STARTED) {
      throw new RssInvalidStateException(String.format(
          "Cannot mark attempt to state %s from its current state %s, %s", targetState, state,
          taskAttemptId));
    }
    state = targetState;
  }

  public void markCommitted() {
    TaskAttemptState targetState = TaskAttemptState.COMMITTED;
    state = targetState;
  }

  public boolean isCommitted() {
    return state == TaskAttemptState.COMMITTED;
  }

  @Override
  public String toString() {
    return "TaskAttemptIdAndState{" +
        ", taskAttemptId=" + taskAttemptId +
        ", state=" + state +
        '}';
  }
}

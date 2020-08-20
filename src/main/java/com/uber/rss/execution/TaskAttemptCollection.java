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

package com.uber.rss.execution;

import com.uber.rss.common.AppMapId;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/***
 * This class contains a collection of task attempts and their states.
 */
public class TaskAttemptCollection {
  private AppMapId appMapId;
  private Map<Long, TaskAttemptIdAndState> tasks = new HashMap<>();

  public TaskAttemptCollection(AppMapId appMapId) {
    this.appMapId = appMapId;
  }

  public TaskAttemptIdAndState getTask(Long taskAttemptId) {
    TaskAttemptIdAndState task = tasks.get(taskAttemptId);
    if (task == null) {
      task = new TaskAttemptIdAndState(appMapId, taskAttemptId);
      tasks.put(taskAttemptId, task);
    }
    return task;
  }

  public TaskAttemptIdAndState getLatestTaskOrNull() {
    if (tasks.isEmpty()) {
      return null;
    }

    return tasks.entrySet().stream()
        .max(new Comparator<Map.Entry<Long, TaskAttemptIdAndState>>() {
          @Override
          public int compare(Map.Entry<Long, TaskAttemptIdAndState> o1, Map.Entry<Long, TaskAttemptIdAndState> o2) {
            return Long.compare(o1.getKey(), o2.getKey());
          }
        })
        .get().getValue();
  }
}

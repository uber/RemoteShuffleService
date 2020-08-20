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

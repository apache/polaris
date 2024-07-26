package io.polaris.service.task;

import io.polaris.core.entity.TaskEntity;

public interface TaskHandler {
  boolean canHandleTask(TaskEntity task);

  boolean handleTask(TaskEntity task);
}

package io.polaris.service.task;

import io.polaris.core.context.CallContext;

/**
 * Execute a task asynchronously with a provided context. The context must be cloned so that callers
 * can close their own context and closables
 */
public interface TaskExecutor {
  void addTaskHandlerContext(long taskEntityId, CallContext callContext);
}

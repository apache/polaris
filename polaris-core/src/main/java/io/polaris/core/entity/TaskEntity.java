package io.polaris.core.entity;

import io.polaris.core.PolarisCallContext;
import io.polaris.core.context.CallContext;
import io.polaris.core.persistence.PolarisObjectMapperUtil;

/**
 * Represents an asynchronous task entity in the persistence layer. A task executor is responsible
 * for constructing the actual task instance based on the "data" and "taskType" properties
 */
public class TaskEntity extends PolarisEntity {
  public TaskEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
  }

  public static TaskEntity of(PolarisBaseEntity polarisEntity) {
    if (polarisEntity != null) {
      return new TaskEntity(polarisEntity);
    } else {
      return null;
    }
  }

  public <T> T readData(Class<T> klass) {
    PolarisCallContext polarisCallContext = CallContext.getCurrentContext().getPolarisCallContext();
    return PolarisObjectMapperUtil.deserialize(
        polarisCallContext, getPropertiesAsMap().get(PolarisTaskConstants.TASK_DATA), klass);
  }

  public AsyncTaskType getTaskType() {
    PolarisCallContext polarisCallContext = CallContext.getCurrentContext().getPolarisCallContext();
    return PolarisObjectMapperUtil.deserialize(
        polarisCallContext,
        getPropertiesAsMap().get(PolarisTaskConstants.TASK_TYPE),
        AsyncTaskType.class);
  }

  public static class Builder extends PolarisEntity.BaseBuilder<TaskEntity, TaskEntity.Builder> {
    public Builder() {
      super();
      setType(PolarisEntityType.TASK);
      setCatalogId(PolarisEntityConstants.getNullId());
      setParentId(PolarisEntityConstants.getRootEntityId());
    }

    public Builder(TaskEntity original) {
      super(original);
    }

    public Builder withTaskType(AsyncTaskType taskType) {
      PolarisCallContext polarisCallContext =
          CallContext.getCurrentContext().getPolarisCallContext();
      properties.put(
          PolarisTaskConstants.TASK_TYPE,
          PolarisObjectMapperUtil.serialize(polarisCallContext, taskType));
      return this;
    }

    public Builder withData(Object data) {
      PolarisCallContext polarisCallContext =
          CallContext.getCurrentContext().getPolarisCallContext();
      properties.put(
          PolarisTaskConstants.TASK_DATA,
          PolarisObjectMapperUtil.serialize(polarisCallContext, data));
      return this;
    }

    public Builder withLastAttemptExecutorId(String executorId) {
      properties.put(PolarisTaskConstants.LAST_ATTEMPT_EXECUTOR_ID, executorId);
      return this;
    }

    public Builder withAttemptCount(int count) {
      properties.put(PolarisTaskConstants.ATTEMPT_COUNT, String.valueOf(count));
      return this;
    }

    public Builder withLastAttemptStartedTimestamp(long timestamp) {
      properties.put(PolarisTaskConstants.LAST_ATTEMPT_START_TIME, String.valueOf(timestamp));
      return this;
    }

    public TaskEntity build() {
      return new TaskEntity(buildBase());
    }
  }
}

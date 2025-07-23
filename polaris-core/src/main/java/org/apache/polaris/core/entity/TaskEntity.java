/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.core.entity;

import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;

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
    return polarisCallContext
        .getObjectMapper()
        .deserialize(getPropertiesAsMap().get(PolarisTaskConstants.TASK_DATA), klass);
  }

  public AsyncTaskType getTaskType() {
    PolarisCallContext polarisCallContext = CallContext.getCurrentContext().getPolarisCallContext();
    return polarisCallContext
        .getObjectMapper()
        .deserialize(getPropertiesAsMap().get(PolarisTaskConstants.TASK_TYPE), AsyncTaskType.class);
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
          PolarisTaskConstants.TASK_TYPE, polarisCallContext.getObjectMapper().serialize(taskType));
      return this;
    }

    public Builder withData(Object data) {
      PolarisCallContext polarisCallContext =
          CallContext.getCurrentContext().getPolarisCallContext();
      properties.put(
          PolarisTaskConstants.TASK_DATA, polarisCallContext.getObjectMapper().serialize(data));
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

    @Override
    public TaskEntity build() {
      return new TaskEntity(buildBase());
    }
  }
}

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

import com.google.common.base.Preconditions;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.persistence.PolarisObjectMapperUtil;

/**
 * Represents an asynchronous task entity in the persistence layer. A task executor is responsible
 * for constructing the actual task instance based on the "data" and "taskType" properties
 */
public class TaskEntity extends PolarisEntity {
  public TaskEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
    Preconditions.checkState(
        getType() == PolarisEntityType.TASK, "Invalid entity type: %s", getType());
    Preconditions.checkState(
        getSubType() == PolarisEntitySubType.NULL_SUBTYPE,
        "Invalid entity sub type: %s",
        getSubType());
  }

  public static @Nullable TaskEntity of(@Nullable PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new TaskEntity(sourceEntity);
    }
    return null;
  }

  public <T> T readData(Class<T> klass) {
    return PolarisObjectMapperUtil.deserialize(
        getPropertiesAsMap().get(PolarisTaskConstants.TASK_DATA), klass);
  }

  public AsyncTaskType getTaskType() {
    return PolarisObjectMapperUtil.deserialize(
        getPropertiesAsMap().get(PolarisTaskConstants.TASK_TYPE), AsyncTaskType.class);
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
      properties.put(PolarisTaskConstants.TASK_TYPE, PolarisObjectMapperUtil.serialize(taskType));
      return this;
    }

    public Builder withData(Object data) {
      properties.put(PolarisTaskConstants.TASK_DATA, PolarisObjectMapperUtil.serialize(data));
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

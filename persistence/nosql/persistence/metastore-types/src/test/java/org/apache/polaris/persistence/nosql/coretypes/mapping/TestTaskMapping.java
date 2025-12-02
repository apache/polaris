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

package org.apache.polaris.persistence.nosql.coretypes.mapping;

import static org.apache.polaris.core.entity.PolarisTaskConstants.ATTEMPT_COUNT;
import static org.apache.polaris.core.entity.PolarisTaskConstants.LAST_ATTEMPT_EXECUTOR_ID;
import static org.apache.polaris.core.entity.PolarisTaskConstants.LAST_ATTEMPT_START_TIME;
import static org.apache.polaris.core.entity.PolarisTaskConstants.TASK_DATA;
import static org.apache.polaris.core.entity.PolarisTaskConstants.TASK_TYPE;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.BaseTestMapping.MappingSample.entityWithProperties;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.BaseTestMapping.MappingSample.entityWithProperty;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.TaskMapping.serializeStringCompressed;
import static org.apache.polaris.persistence.nosql.coretypes.realm.ImmediateTasksObj.IMMEDIATE_TASKS_REF_NAME;

import java.time.Instant;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisObjectMapperUtil;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.realm.ImmediateTaskObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.ImmediateTasksObj;
import org.junit.jupiter.params.provider.MethodSource;

@MethodSource("parameters")
public class TestTaskMapping extends BaseTestMapping {
  @Override
  public ObjType containerObjType() {
    return ImmediateTasksObj.TYPE;
  }

  @Override
  public Class<? extends ObjBase> baseObjClass() {
    return null;
  }

  @Override
  public String refName() {
    return IMMEDIATE_TASKS_REF_NAME;
  }

  @Override
  public boolean isCatalogContent() {
    return false;
  }

  @Override
  public boolean isCatalogRelated() {
    return false;
  }

  @Override
  public boolean isWithStorage() {
    return false;
  }

  static Stream<BaseTestParameter> parameters() {
    var taskData = Map.of("task-foo", "bar", "xyz", "abc");
    return Stream.of(
        new BaseTestParameter(
            PolarisEntityType.TASK, PolarisEntitySubType.NULL_SUBTYPE, ImmediateTaskObj.TYPE) {
          @Override
          public ImmediateTaskObj.Builder objBuilder() {
            return ImmediateTaskObj.builder();
          }

          @Override
          public Stream<MappingSample> typeVariations(MappingSample base) {
            return Stream.of(
                base,
                //
                new MappingSample(
                    objBuilder()
                        .from(base.obj())
                        .taskType(AsyncTaskType.MANIFEST_FILE_CLEANUP)
                        .build(),
                    entityWithProperty(
                        base.entity(),
                        TASK_TYPE,
                        PolarisObjectMapperUtil.serialize(AsyncTaskType.MANIFEST_FILE_CLEANUP))),
                new MappingSample(
                    objBuilder()
                        .from(base.obj())
                        .serializedEntity(
                            serializeStringCompressed(PolarisObjectMapperUtil.serialize(taskData)))
                        .build(),
                    entityWithProperty(
                        base.entity(), TASK_DATA, PolarisObjectMapperUtil.serialize(taskData))),
                new MappingSample(
                    objBuilder().from(base.obj()).lastAttemptExecutorId("executor-id").build(),
                    entityWithProperty(base.entity(), LAST_ATTEMPT_EXECUTOR_ID, "executor-id")),
                new MappingSample(
                    objBuilder().from(base.obj()).attemptCount(123).build(),
                    entityWithProperty(base.entity(), ATTEMPT_COUNT, "123")),
                new MappingSample(
                    objBuilder()
                        .from(base.obj())
                        .lastAttemptStartTime(Instant.ofEpochMilli(1234567890))
                        .build(),
                    entityWithProperty(base.entity(), LAST_ATTEMPT_START_TIME, "1234567890")),
                //
                new MappingSample(
                    objBuilder()
                        .from(base.obj())
                        .taskType(AsyncTaskType.MANIFEST_FILE_CLEANUP)
                        .serializedEntity(
                            serializeStringCompressed(PolarisObjectMapperUtil.serialize(taskData)))
                        .lastAttemptExecutorId("executor-id")
                        .attemptCount(123)
                        .lastAttemptStartTime(Instant.ofEpochMilli(1234567890))
                        .build(),
                    entityWithProperties(
                        base.entity(),
                        Map.of(
                            TASK_TYPE,
                            PolarisObjectMapperUtil.serialize(AsyncTaskType.MANIFEST_FILE_CLEANUP),
                            TASK_DATA,
                            PolarisObjectMapperUtil.serialize(taskData),
                            LAST_ATTEMPT_EXECUTOR_ID,
                            "executor-id",
                            ATTEMPT_COUNT,
                            "123",
                            LAST_ATTEMPT_START_TIME,
                            "1234567890")))
                //
                );
          }
        });
  }
}

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

import static org.apache.polaris.persistence.nosql.coretypes.realm.ImmediateTasksObj.IMMEDIATE_TASKS_REF_NAME;

import jakarta.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.persistence.nosql.coretypes.realm.ImmediateTaskObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.ImmediateTasksObj;

final class TaskMapping extends BaseMapping<ImmediateTaskObj, ImmediateTaskObj.Builder> {
  TaskMapping() {
    super(
        ImmediateTaskObj.TYPE,
        ImmediateTasksObj.TYPE,
        IMMEDIATE_TASKS_REF_NAME,
        PolarisEntityType.TASK);
  }

  @Override
  public ImmediateTaskObj.Builder newObjBuilder(@Nonnull PolarisEntitySubType subType) {
    return ImmediateTaskObj.builder();
  }

  @Override
  void mapToObjTypeSpecific(
      ImmediateTaskObj.Builder b,
      @Nonnull PolarisBaseEntity entity,
      Optional<PolarisPrincipalSecrets> principalSecrets,
      Map<String, String> properties,
      Map<String, String> internalProperties) {
    super.mapToObjTypeSpecific(b, entity, principalSecrets, properties, internalProperties);
    var taskTypeCode = properties.remove(PolarisTaskConstants.TASK_TYPE);
    var lastAttemptExecutorId = properties.remove(PolarisTaskConstants.LAST_ATTEMPT_EXECUTOR_ID);
    if (lastAttemptExecutorId != null) {
      b.lastAttemptExecutorId(lastAttemptExecutorId);
    }
    var lastAttemptStartTime = properties.remove(PolarisTaskConstants.LAST_ATTEMPT_START_TIME);
    if (lastAttemptStartTime != null) {
      b.lastAttemptStartTime(Instant.ofEpochMilli(Long.parseLong(lastAttemptStartTime)));
    }
    var attemptCount = properties.remove(PolarisTaskConstants.ATTEMPT_COUNT);
    if (attemptCount != null) {
      b.attemptCount(Integer.parseInt(attemptCount));
    }
    b.taskType(
            Optional.ofNullable(
                taskTypeCode != null
                    ? AsyncTaskType.fromTypeCode(Integer.parseInt(taskTypeCode))
                    : null))
        .serializedEntity(
            Optional.ofNullable(properties.remove("data"))
                .map(TaskMapping::serializeStringCompressed));
  }

  @Override
  void mapToEntityTypeSpecific(
      ImmediateTaskObj o,
      HashMap<String, String> properties,
      HashMap<String, String> internalProperties,
      PolarisEntitySubType subType) {
    super.mapToEntityTypeSpecific(o, properties, internalProperties, subType);
    o.serializedEntity()
        .map(TaskMapping::deserializeStringCompressed)
        .ifPresent(s -> properties.put("data", s));
    o.taskType()
        .ifPresent(
            v -> properties.put(PolarisTaskConstants.TASK_TYPE, Integer.toString(v.typeCode())));
    o.lastAttemptExecutorId()
        .ifPresent(v -> properties.put(PolarisTaskConstants.LAST_ATTEMPT_EXECUTOR_ID, v));
    o.lastAttemptStartTime()
        .ifPresent(
            v ->
                properties.put(
                    PolarisTaskConstants.LAST_ATTEMPT_START_TIME, Long.toString(v.toEpochMilli())));
    o.attemptCount()
        .ifPresent(v -> properties.put(PolarisTaskConstants.ATTEMPT_COUNT, Integer.toString(v)));
  }

  /**
   * Compress a string, for task-entity data, which can be really huge, like full Iceberg
   * manifest-files as base64 encoded in JSON and such.
   */
  static ByteBuffer serializeStringCompressed(String entityAsJson) {
    try (var byteArrayOutputStream = new ByteArrayOutputStream()) {
      try (var gzip = new GZIPOutputStream(byteArrayOutputStream);
          var out = new DataOutputStream(gzip)) {
        out.writeUTF(entityAsJson);
      }
      return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Decompress a string, for task-entity data, which can be really huge, like full Iceberg
   * manifest-files as base64 encoded in JSON and such.
   */
  static String deserializeStringCompressed(ByteBuffer bytes) {
    var byteArray = new byte[bytes.remaining()];
    bytes.duplicate().get(byteArray);
    try (var in = new DataInputStream(new GZIPInputStream(new ByteArrayInputStream(byteArray)))) {
      return in.readUTF();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

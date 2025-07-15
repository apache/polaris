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
package org.apache.polaris.persistence.nosql.coretypes.realm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;

@PolarisImmutable
@JsonSerialize(as = ImmutableImmediateTaskObj.class)
@JsonDeserialize(as = ImmutableImmediateTaskObj.class)
public interface ImmediateTaskObj extends ObjBase {

  ObjType TYPE = new ImmediateTaskObjType();

  @Override
  default ObjType type() {
    return TYPE;
  }

  Optional<AsyncTaskType> taskType();

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<ByteBuffer> serializedEntity();

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<String> lastAttemptExecutorId();

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<Instant> lastAttemptStartTime();

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  OptionalInt attemptCount();

  static Builder builder() {
    return ImmutableImmediateTaskObj.builder();
  }

  final class ImmediateTaskObjType extends AbstractObjType<ImmediateTaskObj> {
    public ImmediateTaskObjType() {
      super("tsk", "Task", ImmediateTaskObj.class);
    }
  }

  @SuppressWarnings("unused")
  interface Builder extends ObjBase.Builder<ImmediateTaskObj, Builder> {
    @CanIgnoreReturnValue
    Builder from(ImmediateTaskObj from);

    @CanIgnoreReturnValue
    Builder taskType(AsyncTaskType taskType);

    @CanIgnoreReturnValue
    Builder taskType(Optional<? extends AsyncTaskType> taskType);

    @CanIgnoreReturnValue
    Builder serializedEntity(ByteBuffer serializedEntity);

    @CanIgnoreReturnValue
    Builder serializedEntity(Optional<? extends ByteBuffer> serializedEntity);

    @CanIgnoreReturnValue
    Builder lastAttemptExecutorId(String lastAttemptExecutorId);

    @CanIgnoreReturnValue
    Builder lastAttemptStartTime(Instant lastAttemptStartTime);

    @CanIgnoreReturnValue
    Builder attemptCount(int attemptCount);
  }
}

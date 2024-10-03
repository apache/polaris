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
package org.apache.polaris.core.persistence;

import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class PolarisObjectMapperUtilTest {

  @Test
  public void testParseTaskState() {
    PolarisBaseEntity entity =
        new PolarisBaseEntity(
            0L, 1L, PolarisEntityType.TASK, PolarisEntitySubType.NULL_SUBTYPE, 0L, "task");
    entity.setProperties(
        "{\"name\": \"my name\", \"lastAttemptExecutorId\": \"the_executor\", \"data\": {\"nestedFields\": "
            + "{\"further_nesting\": \"astring\", \"anArray\": [1, 2, 3, 4]}, \"anotherNestedField\": \"simple string\"}, "
            + "\"lastAttemptStartTime\": \"100\", \"attemptCount\": \"9\"}");
    PolarisObjectMapperUtil.TaskExecutionState state =
        PolarisObjectMapperUtil.parseTaskState(entity);
    Assertions.assertThat(state)
        .isNotNull()
        .returns(100L, PolarisObjectMapperUtil.TaskExecutionState::getLastAttemptStartTime)
        .returns(9, PolarisObjectMapperUtil.TaskExecutionState::getAttemptCount)
        .returns("the_executor", PolarisObjectMapperUtil.TaskExecutionState::getExecutor);
  }

  @Test
  public void testParseTaskStateWithMissingFields() {
    PolarisBaseEntity entity =
        new PolarisBaseEntity(
            0L, 1L, PolarisEntityType.TASK, PolarisEntitySubType.NULL_SUBTYPE, 0L, "task");
    entity.setProperties(
        "{\"name\": \"my name\", \"data\": {\"nestedFields\": "
            + "{\"further_nesting\": \"astring\", \"anArray\": [1, 2, 3, 4]}, \"anotherNestedField\": \"simple string\"}, "
            + "\"attemptCount\": \"5\"}");
    PolarisObjectMapperUtil.TaskExecutionState state =
        PolarisObjectMapperUtil.parseTaskState(entity);
    Assertions.assertThat(state)
        .isNotNull()
        .returns(0L, PolarisObjectMapperUtil.TaskExecutionState::getLastAttemptStartTime)
        .returns(5, PolarisObjectMapperUtil.TaskExecutionState::getAttemptCount)
        .returns(null, PolarisObjectMapperUtil.TaskExecutionState::getExecutor);
  }

  @Test
  public void testParseTaskStateWithInvalidJson() {
    PolarisBaseEntity entity =
        new PolarisBaseEntity(
            0L, 1L, PolarisEntityType.TASK, PolarisEntitySubType.NULL_SUBTYPE, 0L, "task");
    entity.setProperties(
        "{\"name\": \"my name\", \"data\": {\"nestedFields\": "
            + "{\"further_nesting\": \"astring\", \"anArray\": , : \"simple string\"}, ");
    PolarisObjectMapperUtil.TaskExecutionState state =
        PolarisObjectMapperUtil.parseTaskState(entity);
    Assertions.assertThat(state).isNull();
  }
}

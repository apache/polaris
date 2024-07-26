package io.polaris.core.persistence;

import io.polaris.core.entity.PolarisBaseEntity;
import io.polaris.core.entity.PolarisEntitySubType;
import io.polaris.core.entity.PolarisEntityType;
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

package io.polaris.core.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum AsyncTaskType {
  ENTITY_CLEANUP_SCHEDULER(1),
  FILE_CLEANUP(2);

  private final int typeCode;

  AsyncTaskType(int typeCode) {
    this.typeCode = typeCode;
  }

  @JsonValue
  public int typeCode() {
    return typeCode;
  }

  @JsonCreator
  public static AsyncTaskType fromTypeCode(int typeCode) {
    for (AsyncTaskType taskType : AsyncTaskType.values()) {
      if (taskType.typeCode == typeCode) {
        return taskType;
      }
    }
    return null;
  }
}

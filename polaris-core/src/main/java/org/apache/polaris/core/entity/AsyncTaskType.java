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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum AsyncTaskType {
  ENTITY_CLEANUP_SCHEDULER(1),
  MANIFEST_FILE_CLEANUP(2),
  METADATA_FILE_BATCH_CLEANUP(3);

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

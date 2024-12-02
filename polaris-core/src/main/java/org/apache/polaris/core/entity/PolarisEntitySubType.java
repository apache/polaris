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
import jakarta.annotation.Nullable;

/** Subtype for an entity */
public enum PolarisEntitySubType {
  // ANY_SUBTYPE is not stored but is used to indicate that any subtype entities should be
  // returned, for example when doing a list operation or checking if a table like object of
  // name X exists
  ANY_SUBTYPE(-1, null),
  // the NULL value is used when an entity has no subtype, i.e. NOT_APPLICABLE really
  NULL_SUBTYPE(0, null),
  TABLE(2, PolarisEntityType.TABLE_LIKE),
  VIEW(3, PolarisEntityType.TABLE_LIKE);

  // to efficiently map the code of a subtype to its corresponding subtype enum, use a reverse
  // array which is initialized below
  private static final PolarisEntitySubType[] REVERSE_MAPPING_ARRAY;

  static {
    // find max array size
    int maxId = 0;
    for (PolarisEntitySubType entitySubType : PolarisEntitySubType.values()) {
      if (maxId < entitySubType.code) {
        maxId = entitySubType.code;
      }
    }

    // allocate mapping array
    REVERSE_MAPPING_ARRAY = new PolarisEntitySubType[maxId + 1];

    // populate mapping array, only for positive indices
    for (PolarisEntitySubType entitySubType : PolarisEntitySubType.values()) {
      if (entitySubType.code >= 0) {
        REVERSE_MAPPING_ARRAY[entitySubType.code] = entitySubType;
      }
    }
  }

  // unique code associated to that entity subtype
  private final int code;

  // parent type for this entity
  private final PolarisEntityType parentType;

  PolarisEntitySubType(int code, PolarisEntityType parentType) {
    // remember the id of this entity
    this.code = code;
    this.parentType = parentType;
  }

  /**
   * @return the code associated to a subtype, will be stored in FDB
   */
  @JsonValue
  public int getCode() {
    return code;
  }

  /**
   * @return parent type of that entity
   */
  public PolarisEntityType getParentType() {
    return this.parentType;
  }

  /**
   * Given the id of the subtype of an entity, return the subtype associated to it. Return null if
   * not found
   *
   * @param entitySubTypeCode code associated to the entity type
   * @return entity subtype corresponding to that code or null if mapping not found
   */
  @JsonCreator
  public static @Nullable PolarisEntitySubType fromCode(int entitySubTypeCode) {
    // ensure it is within bounds
    if (entitySubTypeCode >= REVERSE_MAPPING_ARRAY.length) {
      return null;
    }

    // get value
    if (entitySubTypeCode >= 0) {
      return REVERSE_MAPPING_ARRAY[entitySubTypeCode];
    } else {
      for (PolarisEntitySubType entitySubType : PolarisEntitySubType.values()) {
        if (entitySubType.code == entitySubTypeCode) {
          return entitySubType;
        }
      }
    }

    return null;
  }
}

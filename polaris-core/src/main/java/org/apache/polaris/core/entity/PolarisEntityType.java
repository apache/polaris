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

/** Types of entities with their id */
public enum PolarisEntityType {
  NULL_TYPE(0, null, false, false),
  ROOT(1, null, false, false),
  PRINCIPAL(2, ROOT, true, false),
  PRINCIPAL_ROLE(3, ROOT, true, false),
  CATALOG(4, ROOT, false, false),
  CATALOG_ROLE(5, CATALOG, true, false),
  NAMESPACE(6, CATALOG, false, true),
  // generic table is either a view or a real table
  TABLE_LIKE(7, NAMESPACE, false, false),
  TASK(8, ROOT, false, false),
  FILE(9, TABLE_LIKE, false, false);

  // to efficiently map a code to its corresponding entity type, use a reverse array which
  // is initialized below
  private static final PolarisEntityType[] REVERSE_MAPPING_ARRAY;

  static {
    // find max array size
    int maxId = 0;
    for (PolarisEntityType entityType : PolarisEntityType.values()) {
      if (maxId < entityType.code) {
        maxId = entityType.code;
      }
    }

    // allocate mapping array
    REVERSE_MAPPING_ARRAY = new PolarisEntityType[maxId + 1];

    // populate mapping array
    for (PolarisEntityType entityType : PolarisEntityType.values()) {
      REVERSE_MAPPING_ARRAY[entityType.code] = entityType;
    }
  }

  // unique id for an entity type
  private final int code;

  // true if this entity is a grantee, i.e. is an entity which can be on the receiving end of
  // a grant. Only roles and principals are grantees
  private final boolean isGrantee;

  // true if the parent entity type can also be the same type (e.g. namespaces)
  private final boolean parentSelfReference;

  // parent entity type, null for an ACCOUNT
  private final PolarisEntityType parentType;

  PolarisEntityType(int id, PolarisEntityType parentType, boolean isGrantee, boolean sefRef) {
    // remember the id of this entity
    this.code = id;
    this.isGrantee = isGrantee;
    this.parentType = parentType;
    this.parentSelfReference = sefRef;
  }

  /**
   * @return the code associated to the specified the entity type, will be stored in FDB
   */
  @JsonValue
  public int getCode() {
    return code;
  }

  /**
   * @return true if this entity is a grantee, i.e. an entity which can receive grants
   */
  public boolean isGrantee() {
    return this.isGrantee;
  }

  /**
   * @return true if this entity can be nested with itself (like a NAMESPACE)
   */
  public boolean isParentSelfReference() {
    return parentSelfReference;
  }

  /**
   * Given the code associated to the type of entity, return the subtype associated to it. Return
   * null if not found
   *
   * @param entityTypeCode code associated to the entity type
   * @return entity type corresponding to that code or null if mapping not found
   */
  @JsonCreator
  public static @Nullable PolarisEntityType fromCode(int entityTypeCode) {
    // ensure it is within bounds
    if (entityTypeCode >= REVERSE_MAPPING_ARRAY.length) {
      return null;
    }

    // get value
    return REVERSE_MAPPING_ARRAY[entityTypeCode];
  }

  /**
   * @return TRUE if this entity is top-level
   */
  public boolean isTopLevel() {
    return (this.parentType == ROOT || this == ROOT);
  }

  /**
   * @return the parent type of this type in the entity hierarchy
   */
  public PolarisEntityType getParentType() {
    return this.parentType;
  }
}

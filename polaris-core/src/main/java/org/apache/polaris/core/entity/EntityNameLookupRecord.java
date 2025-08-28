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
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class EntityNameLookupRecord implements HasEntityId {
  // entity catalog id
  private final long catalogId;

  // id of the entity
  private final long id;

  // parent id of the entity
  private final long parentId;

  // name of the entity
  private final String name;

  // code representing the type of that entity
  private final int typeCode;

  // code representing the subtype of that entity
  private final int subTypeCode;

  @Override
  public long getCatalogId() {
    return catalogId;
  }

  @Override
  public long getId() {
    return id;
  }

  public long getParentId() {
    return parentId;
  }

  public String getName() {
    return name;
  }

  public int getTypeCode() {
    return typeCode;
  }

  public PolarisEntityType getType() {
    return PolarisEntityType.fromCode(this.typeCode);
  }

  public int getSubTypeCode() {
    return subTypeCode;
  }

  public PolarisEntitySubType getSubType() {
    return PolarisEntitySubType.fromCode(this.subTypeCode);
  }

  @JsonCreator
  public EntityNameLookupRecord(
      @JsonProperty("catalogId") long catalogId,
      @JsonProperty("id") long id,
      @JsonProperty("parentId") long parentId,
      @JsonProperty("name") String name,
      @JsonProperty("typeCode") int typeCode,
      @JsonProperty("subTypeCode") int subTypeCode) {
    this.catalogId = catalogId;
    this.id = id;
    this.parentId = parentId;
    this.name = name;
    this.typeCode = typeCode;
    this.subTypeCode = subTypeCode;
  }

  /** Constructor to create the object with provided entity */
  public EntityNameLookupRecord(PolarisBaseEntity entity) {
    this.catalogId = entity.getCatalogId();
    this.id = entity.getId();
    this.parentId = entity.getParentId();
    this.typeCode = entity.getTypeCode();
    this.name = entity.getName();
    this.subTypeCode = entity.getSubTypeCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof EntityNameLookupRecord)) return false;
    EntityNameLookupRecord that = (EntityNameLookupRecord) o;
    return catalogId == that.catalogId
        && id == that.id
        && parentId == that.parentId
        && typeCode == that.typeCode
        && subTypeCode == that.subTypeCode
        && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(catalogId, id, parentId, name, typeCode, subTypeCode);
  }

  @Override
  public String toString() {
    return "PolarisEntitiesActiveRecord{"
        + "catalogId="
        + catalogId
        + ", id="
        + id
        + ", parentId="
        + parentId
        + ", name='"
        + name
        + '\''
        + ", typeCode="
        + typeCode
        + ", subTypeCode="
        + subTypeCode
        + '}';
  }
}

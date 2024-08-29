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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Objects;

/**
 * Core attributes representing basic information about an entity. Change generally means that the
 * entity will be renamed, dropped, re-created, re-parented. Basically any change to the structure
 * of the entity tree. For some operations like updating the entity, change will mean any change,
 * i.e. entity version mismatch.
 */
public class PolarisEntityCore {

  // the id of the catalog associated to that entity. NULL_ID if this entity is top-level like
  // a catalog
  protected long catalogId;

  // the id of the entity which was resolved
  protected long id;

  // the id of the parent of this entity, use 0 for a top-level entity whose parent is the account
  protected long parentId;

  // the type of the entity when it was resolved
  protected int typeCode;

  // the name that this entity had when it was resolved
  protected String name;

  // the version that this entity had when it was resolved
  protected int entityVersion;

  public PolarisEntityCore() {}

  public PolarisEntityCore(
      long catalogId, long id, long parentId, int typeCode, String name, int entityVersion) {
    this.catalogId = catalogId;
    this.id = id;
    this.parentId = parentId;
    this.typeCode = typeCode;
    this.name = name;
    this.entityVersion = entityVersion;
  }

  public PolarisEntityCore(PolarisBaseEntity entity) {
    this.catalogId = entity.getCatalogId();
    this.id = entity.getId();
    this.parentId = entity.getParentId();
    this.typeCode = entity.getTypeCode();
    this.name = entity.getName();
    this.entityVersion = entity.getEntityVersion();
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getParentId() {
    return parentId;
  }

  public void setParentId(long parentId) {
    this.parentId = parentId;
  }

  public int getTypeCode() {
    return typeCode;
  }

  public void setTypeCode(int typeCode) {
    this.typeCode = typeCode;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getEntityVersion() {
    return entityVersion;
  }

  public long getCatalogId() {
    return catalogId;
  }

  public void setCatalogId(long catalogId) {
    this.catalogId = catalogId;
  }

  public void setEntityVersion(int entityVersion) {
    this.entityVersion = entityVersion;
  }

  /**
   * @return the type of this entity
   */
  @JsonIgnore
  public PolarisEntityType getType() {
    return PolarisEntityType.fromCode(this.typeCode);
  }

  /**
   * @return true if this entity cannot be dropped or renamed. Applies to the admin catalog role and
   *     the polaris service admin principal role.
   */
  @JsonIgnore
  public boolean cannotBeDroppedOrRenamed() {
    return (this.typeCode == PolarisEntityType.CATALOG_ROLE.getCode()
            && this.name.equals(PolarisEntityConstants.getNameOfCatalogAdminRole()))
        || (this.typeCode == PolarisEntityType.PRINCIPAL_ROLE.getCode()
            && this.name.equals(PolarisEntityConstants.getNameOfPrincipalServiceAdminRole()));
  }

  /**
   * @return true if this entity is top-level, like a catalog, a principal,
   */
  @JsonIgnore
  public boolean isTopLevel() {
    return this.getType().isTopLevel();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PolarisEntityCore)) {
      return false;
    }
    PolarisEntityCore that = (PolarisEntityCore) o;
    return catalogId == that.catalogId
        && id == that.id
        && parentId == that.parentId
        && typeCode == that.typeCode
        && entityVersion == that.entityVersion
        && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(catalogId, id, parentId, typeCode, name, entityVersion);
  }

  @Override
  public String toString() {
    return "PolarisEntityCore{"
        + "catalogId="
        + catalogId
        + ", id="
        + id
        + ", parentId="
        + parentId
        + ", typeCode="
        + typeCode
        + ", name='"
        + name
        + '\''
        + ", entityVersion="
        + entityVersion
        + '}';
  }
}

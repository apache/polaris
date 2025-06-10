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
  protected final long catalogId;

  // the id of the entity which was resolved
  protected final long id;

  // the id of the parent of this entity, use 0 for a top-level entity whose parent is the account
  protected final long parentId;

  // the type of the entity when it was resolved
  protected final int typeCode;

  // the name that this entity had when it was resolved
  protected final String name;

  // the version that this entity had when it was resolved
  protected final int entityVersion;

  PolarisEntityCore(Builder<?, ?> builder) {
    this.catalogId = builder.catalogId;
    this.id = builder.id;
    this.parentId = builder.parentId;
    this.typeCode = builder.typeCode;
    this.name = builder.name;
    this.entityVersion = builder.entityVersion == 0 ? 1 : builder.entityVersion;
  }

  public static class Builder<T extends PolarisEntityCore, B extends Builder<T, B>> {
    private long catalogId;
    private long id;
    private long parentId;
    private int typeCode;
    private String name;
    private int entityVersion;

    @SuppressWarnings("unchecked")
    B self() {
      return (B) this;
    }

    public B catalogId(long catalogId) {
      this.catalogId = catalogId;
      return self();
    }

    public B id(long id) {
      this.id = id;
      return self();
    }

    public B parentId(long parentId) {
      this.parentId = parentId;
      return self();
    }

    public B typeCode(int typeCode) {
      this.typeCode = typeCode;
      return self();
    }

    public B name(String name) {
      this.name = name;
      return self();
    }

    public B entityVersion(int entityVersion) {
      this.entityVersion = entityVersion;
      return self();
    }

    public Builder() {}

    public Builder(PolarisEntityCore entityCore) {
      this.catalogId = entityCore.catalogId;
      this.id = entityCore.id;
      this.parentId = entityCore.parentId;
      this.typeCode = entityCore.typeCode;
      this.name = entityCore.name;
      this.entityVersion = entityCore.entityVersion;
    }

    public PolarisEntityCore build() {
      return new PolarisEntityCore(this);
    }
  }

  public long getId() {
    return id;
  }

  public long getParentId() {
    return parentId;
  }

  public int getTypeCode() {
    return typeCode;
  }

  public String getName() {
    return name;
  }

  public int getEntityVersion() {
    return entityVersion;
  }

  public long getCatalogId() {
    return catalogId;
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

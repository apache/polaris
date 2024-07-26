/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package io.polaris.core.entity;

public class PolarisEntitiesActiveKey {

  // entity catalog id
  private final long catalogId;

  // parent id of the entity
  private final long parentId;

  // code representing the type of that entity
  private final int typeCode;

  // name of the entity
  private final String name;

  public PolarisEntitiesActiveKey(long catalogId, long parentId, int typeCode, String name) {
    this.catalogId = catalogId;
    this.parentId = parentId;
    this.typeCode = typeCode;
    this.name = name;
  }

  /** Constructor to create the object with provided entity */
  public PolarisEntitiesActiveKey(PolarisEntityCore entity) {
    this.catalogId = entity.getCatalogId();
    this.parentId = entity.getParentId();
    this.typeCode = entity.getTypeCode();
    this.name = entity.getName();
  }

  public long getCatalogId() {
    return catalogId;
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

  @Override
  public String toString() {
    return "PolarisEntitiesActiveKey{"
        + "catalogId="
        + catalogId
        + ", parentId="
        + parentId
        + ", typeCode="
        + typeCode
        + ", name='"
        + name
        + '\''
        + '}';
  }
}

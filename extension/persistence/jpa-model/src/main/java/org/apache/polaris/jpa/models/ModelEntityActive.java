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
package org.apache.polaris.jpa.models;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.apache.polaris.core.entity.PolarisEntityActiveRecord;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;

/**
 * EntityActive model representing some attributes of a Polaris Entity. This is used to exchange
 * entity information with ENTITIES_ACTIVE table
 */
@Entity
@Table(name = "ENTITIES_ACTIVE")
public class ModelEntityActive {
  // entity catalog id
  @Id private long catalogId;

  // id of the entity
  @Id private long id;

  // parent id of the entity
  @Id private long parentId;

  // name of the entity
  private String name;

  // code representing the type of that entity
  @Id private int typeCode;

  // code representing the subtype of that entity
  private int subTypeCode;

  public long getCatalogId() {
    return catalogId;
  }

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

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private final ModelEntityActive entity;

    private Builder() {
      entity = new ModelEntityActive();
    }

    public Builder catalogId(long catalogId) {
      entity.catalogId = catalogId;
      return this;
    }

    public Builder id(long id) {
      entity.id = id;
      return this;
    }

    public Builder parentId(long parentId) {
      entity.parentId = parentId;
      return this;
    }

    public Builder typeCode(int typeCode) {
      entity.typeCode = typeCode;
      return this;
    }

    public Builder name(String name) {
      entity.name = name;
      return this;
    }

    public Builder subTypeCode(int subTypeCode) {
      entity.subTypeCode = subTypeCode;
      return this;
    }

    public ModelEntityActive build() {
      return entity;
    }
  }

  public static ModelEntityActive fromEntityActive(PolarisEntityActiveRecord record) {
    return ModelEntityActive.builder()
        .catalogId(record.getCatalogId())
        .id(record.getId())
        .parentId(record.getParentId())
        .name(record.getName())
        .typeCode(record.getTypeCode())
        .subTypeCode(record.getSubTypeCode())
        .build();
  }

  public static PolarisEntityActiveRecord toEntityActive(ModelEntityActive model) {
    if (model == null) {
      return null;
    }

    return new PolarisEntityActiveRecord(
        model.catalogId, model.id, model.parentId, model.name, model.typeCode, model.subTypeCode);
  }
}

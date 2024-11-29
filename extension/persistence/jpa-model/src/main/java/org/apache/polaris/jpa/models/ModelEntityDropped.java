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
import jakarta.persistence.Version;
import org.apache.polaris.core.entity.PolarisBaseEntity;

/**
 * EntityDropped model representing some attributes of a Polaris Entity. This is used to exchange
 * entity information with ENTITIES_DROPPED table
 */
@Entity
@Table(name = "ENTITIES_DROPPED")
public class ModelEntityDropped {
  // the id of the catalog associated to that entity. NULL_ID if this entity is top-level like
  // a catalog
  @Id private long catalogId;

  // the id of the entity which was resolved
  private long id;

  // the id of the parent of this entity, use 0 for a top-level entity whose parent is the account
  @Id private long parentId;

  // the type of the entity when it was resolved
  @Id private int typeCode;

  // the name that this entity had when it was resolved
  @Id private String name;

  // the type of the entity when it was resolved
  @Id private int subTypeCode;

  // when this entity was dropped. Null if was never dropped
  @Id private long dropTimestamp;

  // when should we start purging this entity
  private long toPurgeTimestamp;

  // Used for Optimistic Locking to handle concurrent reads and updates
  @Version private long version;

  public long getCatalogId() {
    return catalogId;
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

  public int getSubTypeCode() {
    return subTypeCode;
  }

  public long getDropTimestamp() {
    return dropTimestamp;
  }

  public long getToPurgeTimestamp() {
    return toPurgeTimestamp;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private final ModelEntityDropped entity;

    private Builder() {
      entity = new ModelEntityDropped();
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

    public Builder dropTimestamp(long dropTimestamp) {
      entity.dropTimestamp = dropTimestamp;
      return this;
    }

    public Builder toPurgeTimestamp(long toPurgeTimestamp) {
      entity.toPurgeTimestamp = toPurgeTimestamp;
      return this;
    }

    public ModelEntityDropped build() {
      return entity;
    }
  }

  public static ModelEntityDropped fromEntity(PolarisBaseEntity entity) {
    if (entity == null) return null;

    return ModelEntityDropped.builder()
        .catalogId(entity.getCatalogId())
        .id(entity.getId())
        .parentId(entity.getParentId())
        .typeCode(entity.getTypeCode())
        .name(entity.getName())
        .subTypeCode(entity.getSubTypeCode())
        .dropTimestamp(entity.getDropTimestamp())
        .toPurgeTimestamp(entity.getToPurgeTimestamp())
        .build();
  }
}

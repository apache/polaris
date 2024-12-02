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

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;

/**
 * Entity model representing all attributes of a Polaris Entity. This is used to exchange full
 * entity information with ENTITIES table
 */
@Entity
@Table(name = "ENTITIES")
public class ModelEntity {
  // the id of the catalog associated to that entity. NULL_ID if this entity is top-level like
  // a catalog
  @Id private long catalogId;

  // the id of the entity which was resolved
  @Id private long id;

  // the id of the parent of this entity, use 0 for a top-level entity whose parent is the account
  private long parentId;

  // the type of the entity when it was resolved
  private int typeCode;

  // the name that this entity had when it was resolved
  private String name;

  // the version that this entity had when it was resolved
  private int entityVersion;

  public static final String EMPTY_MAP_STRING = "{}";
  // the type of the entity when it was resolved
  private int subTypeCode;

  // timestamp when this entity was created
  private long createTimestamp;

  // when this entity was dropped. Null if was never dropped
  private long dropTimestamp;

  // when did we start purging this entity. When not null, un-drop is no longer possible
  private long purgeTimestamp;

  // when should we start purging this entity
  private long toPurgeTimestamp;

  // last time this entity was updated, only for troubleshooting
  private long lastUpdateTimestamp;

  // properties, serialized as a JSON string
  @Column(columnDefinition = "TEXT")
  private String properties;

  // internal properties, serialized as a JSON string
  @Column(columnDefinition = "TEXT")
  private String internalProperties;

  // current version for that entity, will be monotonically incremented
  private int grantRecordsVersion;

  // Used for Optimistic Locking to handle concurrent reads and updates
  @Version private long version;

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

  public int getSubTypeCode() {
    return subTypeCode;
  }

  public long getCreateTimestamp() {
    return createTimestamp;
  }

  public long getDropTimestamp() {
    return dropTimestamp;
  }

  public long getPurgeTimestamp() {
    return purgeTimestamp;
  }

  public long getToPurgeTimestamp() {
    return toPurgeTimestamp;
  }

  public long getLastUpdateTimestamp() {
    return lastUpdateTimestamp;
  }

  public String getProperties() {
    return properties != null ? properties : EMPTY_MAP_STRING;
  }

  public String getInternalProperties() {
    return internalProperties != null ? internalProperties : EMPTY_MAP_STRING;
  }

  public int getGrantRecordsVersion() {
    return grantRecordsVersion;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private final ModelEntity entity;

    private Builder() {
      entity = new ModelEntity();
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

    public Builder entityVersion(int entityVersion) {
      entity.entityVersion = entityVersion;
      return this;
    }

    public Builder subTypeCode(int subTypeCode) {
      entity.subTypeCode = subTypeCode;
      return this;
    }

    public Builder createTimestamp(long createTimestamp) {
      entity.createTimestamp = createTimestamp;
      return this;
    }

    public Builder dropTimestamp(long dropTimestamp) {
      entity.dropTimestamp = dropTimestamp;
      return this;
    }

    public Builder purgeTimestamp(long purgeTimestamp) {
      entity.purgeTimestamp = purgeTimestamp;
      return this;
    }

    public Builder toPurgeTimestamp(long toPurgeTimestamp) {
      entity.toPurgeTimestamp = toPurgeTimestamp;
      return this;
    }

    public Builder lastUpdateTimestamp(long lastUpdateTimestamp) {
      entity.lastUpdateTimestamp = lastUpdateTimestamp;
      return this;
    }

    public Builder properties(String properties) {
      entity.properties = properties;
      return this;
    }

    public Builder internalProperties(String internalProperties) {
      entity.internalProperties = internalProperties;
      return this;
    }

    public Builder grantRecordsVersion(int grantRecordsVersion) {
      entity.grantRecordsVersion = grantRecordsVersion;
      return this;
    }

    public ModelEntity build() {
      return entity;
    }
  }

  public static ModelEntity fromEntity(PolarisBaseEntity entity) {
    return ModelEntity.builder()
        .catalogId(entity.getCatalogId())
        .id(entity.getId())
        .parentId(entity.getParentId())
        .typeCode(entity.getTypeCode())
        .name(entity.getName())
        .entityVersion(entity.getEntityVersion())
        .subTypeCode(entity.getSubTypeCode())
        .createTimestamp(entity.getCreateTimestamp())
        .dropTimestamp(entity.getDropTimestamp())
        .purgeTimestamp(entity.getPurgeTimestamp())
        .toPurgeTimestamp(entity.getToPurgeTimestamp())
        .lastUpdateTimestamp(entity.getLastUpdateTimestamp())
        .properties(entity.getProperties())
        .internalProperties(entity.getInternalProperties())
        .grantRecordsVersion(entity.getGrantRecordsVersion())
        .build();
  }

  public static PolarisBaseEntity toEntity(ModelEntity model) {
    if (model == null) {
      return null;
    }

    var entity =
        new PolarisBaseEntity(
            model.getCatalogId(),
            model.getId(),
            PolarisEntityType.fromCode(model.getTypeCode()),
            PolarisEntitySubType.fromCode(model.getSubTypeCode()),
            model.getParentId(),
            model.getName());
    entity.setEntityVersion(model.getEntityVersion());
    entity.setCreateTimestamp(model.getCreateTimestamp());
    entity.setDropTimestamp(model.getDropTimestamp());
    entity.setPurgeTimestamp(model.getPurgeTimestamp());
    entity.setToPurgeTimestamp(model.getToPurgeTimestamp());
    entity.setLastUpdateTimestamp(model.getLastUpdateTimestamp());
    entity.setProperties(model.getProperties());
    entity.setInternalProperties(model.getInternalProperties());
    entity.setGrantRecordsVersion(model.getGrantRecordsVersion());
    return entity;
  }

  public void update(PolarisBaseEntity entity) {
    if (entity == null) return;

    this.catalogId = entity.getCatalogId();
    this.id = entity.getId();
    this.parentId = entity.getParentId();
    this.typeCode = entity.getTypeCode();
    this.name = entity.getName();
    this.entityVersion = entity.getEntityVersion();
    this.subTypeCode = entity.getSubTypeCode();
    this.createTimestamp = entity.getCreateTimestamp();
    this.dropTimestamp = entity.getDropTimestamp();
    this.purgeTimestamp = entity.getPurgeTimestamp();
    this.toPurgeTimestamp = entity.getToPurgeTimestamp();
    this.lastUpdateTimestamp = entity.getLastUpdateTimestamp();
    this.properties = entity.getProperties();
    this.internalProperties = entity.getInternalProperties();
    this.grantRecordsVersion = entity.getGrantRecordsVersion();
  }
}

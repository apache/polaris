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
package org.apache.polaris.persistence.relational.jdbc.models;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;

public class ModelEntity implements Converter<PolarisBaseEntity> {
  public static final String TABLE_NAME = "ENTITIES";

  public static final String ID_COLUMN = "id";

  public static final List<String> ALL_COLUMNS =
      List.of(
          "id",
          "catalog_id",
          "parent_id",
          "type_code",
          "name",
          "entity_version",
          "sub_type_code",
          "create_timestamp",
          "drop_timestamp",
          "purge_timestamp",
          "to_purge_timestamp",
          "last_update_timestamp",
          "properties",
          "internal_properties",
          "grant_records_version",
          "location_without_scheme");

  // the id of the catalog associated to that entity. use 0 if this entity is top-level
  // like a catalog
  private long catalogId;

  // the id of the entity which was resolved
  private long id;

  // the id of the parent of this entity, use 0 for a top-level entity whose parent is the account
  private long parentId;

  // the type of the entity when it was resolved
  private int typeCode;

  // the name that this entity had when it was resolved
  private String name;

  // the version that this entity had when it was resolved
  private int entityVersion;

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

  // last time this entity was updated
  private long lastUpdateTimestamp;

  // properties, serialized as a JSON string
  private String properties;

  // internal properties, serialized as a JSON string
  private String internalProperties;

  // current version for that entity, will be monotonically incremented
  private int grantRecordsVersion;

  // location for the entity but without a scheme, when applicable
  private String locationWithoutScheme;

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
    return properties != null ? properties : "{}";
  }

  public String getInternalProperties() {
    return internalProperties != null ? internalProperties : "{}";
  }

  public int getGrantRecordsVersion() {
    return grantRecordsVersion;
  }

  public String getLocationWithoutScheme() {
    return locationWithoutScheme;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public PolarisBaseEntity fromResultSet(ResultSet r) throws SQLException {
    var modelEntity =
        ModelEntity.builder()
            .catalogId(r.getObject("catalog_id", Long.class))
            .id(r.getObject("id", Long.class))
            .parentId(r.getObject("parent_id", Long.class))
            .typeCode(r.getObject("type_code", Integer.class))
            .name(r.getObject("name", String.class))
            .entityVersion(r.getObject("entity_version", Integer.class))
            .subTypeCode(r.getObject("sub_type_code", Integer.class))
            .createTimestamp(r.getObject("create_timestamp", Long.class))
            .dropTimestamp(r.getObject("drop_timestamp", Long.class))
            .purgeTimestamp(r.getObject("purge_timestamp", Long.class))
            .toPurgeTimestamp(r.getObject("to_purge_timestamp", Long.class))
            .lastUpdateTimestamp(r.getObject("last_update_timestamp", Long.class))
            // JSONB: use getString(), not getObject().
            .properties(r.getString("properties"))
            // JSONB: use getString(), not getObject().
            .internalProperties(r.getString("internal_properties"))
            .grantRecordsVersion(r.getObject("grant_records_version", Integer.class))
            .locationWithoutScheme(r.getString("location_without_scheme"))
            .build();

    return toEntity(modelEntity);
  }

  @Override
  public Map<String, Object> toMap(DatabaseType databaseType) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("id", this.getId());
    map.put("catalog_id", this.getCatalogId());
    map.put("parent_id", this.getParentId());
    map.put("type_code", this.getTypeCode());
    map.put("name", this.getName());
    map.put("entity_version", this.getEntityVersion());
    map.put("sub_type_code", this.getSubTypeCode());
    map.put("create_timestamp", this.getCreateTimestamp());
    map.put("drop_timestamp", this.getDropTimestamp());
    map.put("purge_timestamp", this.getPurgeTimestamp());
    map.put("to_purge_timestamp", this.getToPurgeTimestamp());
    map.put("last_update_timestamp", this.getLastUpdateTimestamp());
    if (databaseType.equals(DatabaseType.POSTGRES)) {
      map.put("properties", toJsonbPGobject(this.getProperties()));
      map.put("internal_properties", toJsonbPGobject(this.getInternalProperties()));
    } else {
      map.put("properties", this.getProperties());
      map.put("internal_properties", this.getInternalProperties());
    }
    map.put("grant_records_version", this.getGrantRecordsVersion());
    map.put("location_without_scheme", this.getLocationWithoutScheme());
    return map;
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

    public Builder locationWithoutScheme(String location) {
      entity.locationWithoutScheme = location;
      return this;
    }

    public ModelEntity build() {
      return entity;
    }
  }

  public static ModelEntity fromEntity(PolarisBaseEntity entity) {
    var builder =
        ModelEntity.builder()
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
            .grantRecordsVersion(entity.getGrantRecordsVersion());

    if (entity.getType() == PolarisEntityType.TABLE_LIKE) {
      if (entity.getSubType() == PolarisEntitySubType.ICEBERG_TABLE
          || entity.getSubType() == PolarisEntitySubType.ICEBERG_VIEW) {
        builder.locationWithoutScheme(
            StorageLocation.of(
                    entity.getPropertiesAsMap().get(PolarisEntityConstants.ENTITY_BASE_LOCATION))
                .withoutScheme());
      }
    }
    if (entity.getType() == PolarisEntityType.NAMESPACE) {
      builder.locationWithoutScheme(
          StorageLocation.of(
                  entity.getPropertiesAsMap().get(PolarisEntityConstants.ENTITY_BASE_LOCATION))
              .withoutScheme());
    }

    return builder.build();
  }

  public static PolarisBaseEntity toEntity(ModelEntity model) {
    if (model == null) {
      return null;
    }

    PolarisEntityType entityType = PolarisEntityType.fromCode(model.getTypeCode());
    PolarisEntitySubType subType = PolarisEntitySubType.fromCode(model.getSubTypeCode());

    if (entityType == null) {
      throw new IllegalArgumentException("Invalid entity type: " + model.getTypeCode());
    }

    if (subType == null) {
      throw new IllegalArgumentException("Invalid entity subtype: " + model.getSubTypeCode());
    }

    return new PolarisBaseEntity.Builder()
        .catalogId(model.getCatalogId())
        .id(model.getId())
        .typeCode(model.getTypeCode())
        .subTypeCode(model.getSubTypeCode())
        .parentId(model.getParentId())
        .name(model.getName())
        .entityVersion(model.getEntityVersion())
        .createTimestamp(model.getCreateTimestamp())
        .dropTimestamp(model.getDropTimestamp())
        .purgeTimestamp(model.getPurgeTimestamp())
        .toPurgeTimestamp(model.getToPurgeTimestamp())
        .lastUpdateTimestamp(model.getLastUpdateTimestamp())
        .properties(model.getProperties())
        .internalProperties(model.getInternalProperties())
        .grantRecordsVersion(model.getGrantRecordsVersion())
        .build();
  }
}

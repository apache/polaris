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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;

public class PolarisEntity extends PolarisBaseEntity {

  public static class NameAndId {
    private final String name;
    private final long id;

    public NameAndId(String name, long id) {
      this.name = name;
      this.id = id;
    }

    public String getName() {
      return name;
    }

    public long getId() {
      return id;
    }
  }

  public static class TypeSubTypeAndName {
    private final PolarisEntityType type;
    private final PolarisEntitySubType subType;
    private final String name;

    public TypeSubTypeAndName(PolarisEntityType type, PolarisEntitySubType subType, String name) {
      this.type = type;
      this.subType = subType;
      this.name = name;
    }

    public PolarisEntityType getType() {
      return type;
    }

    public PolarisEntitySubType getSubType() {
      return subType;
    }

    public String getName() {
      return name;
    }
  }

  @JsonCreator
  private PolarisEntity(
      @JsonProperty("catalogId") long catalogId,
      @JsonProperty("typeCode") PolarisEntityType type,
      @JsonProperty("subTypeCode") PolarisEntitySubType subType,
      @JsonProperty("id") long id,
      @JsonProperty("parentId") long parentId,
      @JsonProperty("name") String name,
      @JsonProperty("createTimestamp") long createTimestamp,
      @JsonProperty("dropTimestamp") long dropTimestamp,
      @JsonProperty("purgeTimestamp") long purgeTimestamp,
      @JsonProperty("lastUpdateTimestamp") long lastUpdateTimestamp,
      @JsonProperty("properties") String properties,
      @JsonProperty("internalProperties") String internalProperties,
      @JsonProperty("entityVersion") int entityVersion,
      @JsonProperty("grantRecordsVersion") int grantRecordsVersion) {
    super(catalogId, id, type, subType, parentId, name);
    this.createTimestamp = createTimestamp;
    this.dropTimestamp = dropTimestamp;
    this.purgeTimestamp = purgeTimestamp;
    this.lastUpdateTimestamp = lastUpdateTimestamp;
    this.properties = properties;
    this.internalProperties = internalProperties;
    this.entityVersion = entityVersion;
    this.grantRecordsVersion = grantRecordsVersion;
  }

  public PolarisEntity(
      long catalogId,
      PolarisEntityType type,
      PolarisEntitySubType subType,
      long id,
      long parentId,
      String name,
      long createTimestamp,
      long dropTimestamp,
      long purgeTimestamp,
      long lastUpdateTimestamp,
      Map<String, String> properties,
      Map<String, String> internalProperties,
      int entityVersion,
      int grantRecordsVersion) {
    super(catalogId, id, type, subType, parentId, name);
    this.createTimestamp = createTimestamp;
    this.dropTimestamp = dropTimestamp;
    this.purgeTimestamp = purgeTimestamp;
    this.lastUpdateTimestamp = lastUpdateTimestamp;
    this.setPropertiesAsMap(properties);
    this.setInternalPropertiesAsMap(internalProperties);
    this.entityVersion = entityVersion;
    this.grantRecordsVersion = grantRecordsVersion;
  }

  public static PolarisEntity of(PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new PolarisEntity(sourceEntity);
    }
    return null;
  }

  public static PolarisEntity of(PolarisMetaStoreManager.EntityResult result) {
    if (result.isSuccess()) {
      return new PolarisEntity(result.getEntity());
    }
    return null;
  }

  public static PolarisEntityCore toCore(PolarisBaseEntity entity) {
    PolarisEntityCore entityCore =
        new PolarisEntityCore(
            entity.getCatalogId(),
            entity.getId(),
            entity.getParentId(),
            entity.getTypeCode(),
            entity.getName(),
            entity.getEntityVersion());
    return entityCore;
  }

  public static List<PolarisEntityCore> toCoreList(List<PolarisEntity> path) {
    return Optional.ofNullable(path)
        .filter(Predicate.not(List::isEmpty))
        .map(list -> list.stream().map(PolarisEntity::toCore).collect(Collectors.toList()))
        .orElse(null);
  }

  public static List<NameAndId> toNameAndIdList(List<PolarisEntityActiveRecord> entities) {
    return Optional.ofNullable(entities)
        .map(
            list ->
                list.stream()
                    .map(record -> new NameAndId(record.getName(), record.getId()))
                    .collect(Collectors.toList()))
        .orElse(null);
  }

  public PolarisEntity(@Nonnull PolarisBaseEntity sourceEntity) {
    super(
        sourceEntity.getCatalogId(),
        sourceEntity.getId(),
        sourceEntity.getTypeCode(),
        sourceEntity.getSubTypeCode(),
        sourceEntity.getParentId(),
        sourceEntity.getName());
    this.createTimestamp = sourceEntity.getCreateTimestamp();
    this.dropTimestamp = sourceEntity.getDropTimestamp();
    this.purgeTimestamp = sourceEntity.getPurgeTimestamp();
    this.lastUpdateTimestamp = sourceEntity.getLastUpdateTimestamp();
    this.properties = sourceEntity.getProperties();
    this.internalProperties = sourceEntity.getInternalProperties();
    this.entityVersion = sourceEntity.getEntityVersion();
    this.grantRecordsVersion = sourceEntity.getGrantRecordsVersion();
  }

  @JsonIgnore
  @Override
  public PolarisEntityType getType() {
    return PolarisEntityType.fromCode(getTypeCode());
  }

  @JsonIgnore
  @Override
  public PolarisEntitySubType getSubType() {
    return PolarisEntitySubType.fromCode(getSubTypeCode());
  }

  @JsonIgnore
  public NameAndId nameAndId() {
    return new NameAndId(name, id);
  }

  @Override
  public String toString() {
    return "name="
        + getName()
        + ";id="
        + getId()
        + ";parentId="
        + getParentId()
        + ";entityVersion="
        + getEntityVersion()
        + ";type="
        + getType()
        + ";subType="
        + getSubType()
        + ";internalProperties="
        + getInternalPropertiesAsMap();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PolarisEntity)) return false;
    PolarisEntity that = (PolarisEntity) o;
    return catalogId == that.catalogId
        && id == that.id
        && parentId == that.parentId
        && createTimestamp == that.createTimestamp
        && dropTimestamp == that.dropTimestamp
        && purgeTimestamp == that.purgeTimestamp
        && lastUpdateTimestamp == that.lastUpdateTimestamp
        && entityVersion == that.entityVersion
        && grantRecordsVersion == that.grantRecordsVersion
        && typeCode == that.typeCode
        && subTypeCode == that.subTypeCode
        && Objects.equals(name, that.name)
        && Objects.equals(properties, that.properties)
        && Objects.equals(internalProperties, that.internalProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        typeCode,
        subTypeCode,
        catalogId,
        id,
        parentId,
        name,
        createTimestamp,
        dropTimestamp,
        purgeTimestamp,
        lastUpdateTimestamp,
        properties,
        internalProperties,
        entityVersion,
        grantRecordsVersion);
  }

  public static class Builder extends BaseBuilder<PolarisEntity, Builder> {
    public Builder() {
      super();
    }

    public Builder(PolarisEntity original) {
      super(original);
    }

    @Override
    public PolarisEntity build() {
      return buildBase();
    }
  }

  @SuppressWarnings("unchecked")
  public abstract static class BaseBuilder<T extends PolarisEntity, B extends BaseBuilder<T, B>> {
    protected long catalogId;
    protected PolarisEntityType type;
    protected PolarisEntitySubType subType;
    protected long id;
    protected long parentId;
    protected String name;
    protected long createTimestamp;
    protected long dropTimestamp;
    protected long purgeTimestamp;
    protected long lastUpdateTimestamp;
    protected Map<String, String> properties;
    protected Map<String, String> internalProperties;
    protected int entityVersion;
    protected int grantRecordsVersion;

    protected BaseBuilder() {
      this.catalogId = -1;
      this.type = PolarisEntityType.NULL_TYPE;
      this.subType = PolarisEntitySubType.NULL_SUBTYPE;
      this.id = -1;
      this.parentId = 0;
      this.name = null;
      this.createTimestamp = 0;
      this.dropTimestamp = 0;
      this.purgeTimestamp = 0;
      this.lastUpdateTimestamp = 0;
      this.properties = new HashMap<>();
      this.internalProperties = new HashMap<>();
      this.entityVersion = 1;
      this.grantRecordsVersion = 1;
    }

    protected BaseBuilder(T original) {
      this.catalogId = original.catalogId;
      this.type = original.getType();
      this.subType = original.getSubType();
      this.id = original.id;
      this.parentId = original.parentId;
      this.name = original.name;
      this.createTimestamp = original.createTimestamp;
      this.dropTimestamp = original.dropTimestamp;
      this.purgeTimestamp = original.purgeTimestamp;
      this.lastUpdateTimestamp = original.lastUpdateTimestamp;
      this.properties = new HashMap<>(original.getPropertiesAsMap());
      this.internalProperties = new HashMap<>(original.getInternalPropertiesAsMap());
      this.entityVersion = original.entityVersion;
      this.grantRecordsVersion = original.grantRecordsVersion;
    }

    public abstract T build();

    public PolarisEntity buildBase() {
      // TODO: Validate required fields
      // id > 0 already -- client must always supply id for idempotency purposes.
      return new PolarisEntity(
          catalogId,
          type,
          subType,
          id,
          parentId,
          name,
          createTimestamp,
          dropTimestamp,
          purgeTimestamp,
          lastUpdateTimestamp,
          properties,
          internalProperties,
          entityVersion,
          grantRecordsVersion);
    }

    public B setCatalogId(long catalogId) {
      this.catalogId = catalogId;
      return (B) this;
    }

    public B setType(PolarisEntityType type) {
      this.type = type;
      return (B) this;
    }

    public B setSubType(PolarisEntitySubType subType) {
      this.subType = subType;
      return (B) this;
    }

    public B setId(long id) {
      // TODO: Maybe block this one whenever builder is created from previously-existing entity
      // since re-opening an entity should only be for modifying the mutable fields for a given
      // logical entity. Would require separate builder type for "clone"-style copies, but
      // usually when creating from other entity we want to preserve the id.
      this.id = id;
      return (B) this;
    }

    public B setParentId(long parentId) {
      this.parentId = parentId;
      return (B) this;
    }

    public B setName(String name) {
      this.name = name;
      return (B) this;
    }

    public B setCreateTimestamp(long createTimestamp) {
      this.createTimestamp = createTimestamp;
      return (B) this;
    }

    public B setDropTimestamp(long dropTimestamp) {
      this.dropTimestamp = dropTimestamp;
      return (B) this;
    }

    public B setPurgeTimestamp(long purgeTimestamp) {
      this.purgeTimestamp = purgeTimestamp;
      return (B) this;
    }

    public B setLastUpdateTimestamp(long lastUpdateTimestamp) {
      this.lastUpdateTimestamp = lastUpdateTimestamp;
      return (B) this;
    }

    public B setProperties(Map<String, String> properties) {
      this.properties = new HashMap<>(properties);
      return (B) this;
    }

    public B addProperty(String key, String value) {
      this.properties.put(key, value);
      return (B) this;
    }

    public B setInternalProperties(Map<String, String> internalProperties) {
      this.internalProperties = new HashMap<>(internalProperties);
      return (B) this;
    }

    public B setEntityVersion(int entityVersion) {
      this.entityVersion = entityVersion;
      return (B) this;
    }

    public B setGrantRecordsVersion(int grantRecordsVersion) {
      this.grantRecordsVersion = grantRecordsVersion;
      return (B) this;
    }
  }
}

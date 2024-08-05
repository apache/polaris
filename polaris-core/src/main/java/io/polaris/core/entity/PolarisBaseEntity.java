/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.core.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.polaris.core.PolarisUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Base polaris entity representing all attributes of a Polaris Entity. This is used to exchange
 * full entity information between the client and the GS backend
 */
public class PolarisBaseEntity extends PolarisEntityCore {

  public static final String EMPTY_MAP_STRING = "{}";

  // to serialize/deserialize properties
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // the type of the entity when it was resolved
  protected int subTypeCode;

  // timestamp when this entity was created
  protected long createTimestamp;

  // when this entity was dropped. Null if was never dropped
  protected long dropTimestamp;

  // when did we start purging this entity. When not null, un-drop is no longer possible
  protected long purgeTimestamp;

  // when should we start purging this entity
  protected long toPurgeTimestamp;

  // last time this entity was updated, only for troubleshooting
  protected long lastUpdateTimestamp;

  // properties, serialized as a JSON string
  protected String properties;

  // internal properties, serialized as a JSON string
  protected String internalProperties;

  // current version for that entity, will be monotonically incremented
  protected int grantRecordsVersion;

  public int getSubTypeCode() {
    return subTypeCode;
  }

  public void setSubTypeCode(int subTypeCode) {
    this.subTypeCode = subTypeCode;
  }

  public long getCreateTimestamp() {
    return createTimestamp;
  }

  public void setCreateTimestamp(long createTimestamp) {
    this.createTimestamp = createTimestamp;
  }

  public long getDropTimestamp() {
    return dropTimestamp;
  }

  public void setDropTimestamp(long dropTimestamp) {
    this.dropTimestamp = dropTimestamp;
  }

  public long getPurgeTimestamp() {
    return purgeTimestamp;
  }

  public void setPurgeTimestamp(long purgeTimestamp) {
    this.purgeTimestamp = purgeTimestamp;
  }

  public long getToPurgeTimestamp() {
    return toPurgeTimestamp;
  }

  public void setToPurgeTimestamp(long toPurgeTimestamp) {
    this.toPurgeTimestamp = toPurgeTimestamp;
  }

  public long getLastUpdateTimestamp() {
    return lastUpdateTimestamp;
  }

  public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
    this.lastUpdateTimestamp = lastUpdateTimestamp;
  }

  public String getProperties() {
    return properties != null ? properties : EMPTY_MAP_STRING;
  }

  public String getLocation() {
    if (getType() == PolarisEntityType.TABLE_LIKE) {
      return PolarisUtils.terminateWithSlash(
          getPropertiesAsMap().get(PolarisEntityConstants.ENTITY_BASE_LOCATION));
    } else {
      return null;
    }
  }

  @JsonIgnore
  public Map<String, String> getPropertiesAsMap() {
    if (properties == null) {
      return new HashMap<>();
    }
    try {
      return MAPPER.readValue(properties, new TypeReference<>() {});
    } catch (JsonProcessingException ex) {
      throw new IllegalStateException(
          String.format("Failed to deserialize json. properties %s", properties), ex);
    }
  }

  /**
   * Set one single property
   *
   * @param propName name of the property
   * @param propValue value of that property
   */
  public void addProperty(String propName, String propValue) {
    Map<String, String> props = this.getPropertiesAsMap();
    props.put(propName, propValue);
    this.setPropertiesAsMap(props);
  }

  public void setProperties(String properties) {
    this.properties = properties;
  }

  @JsonIgnore
  public void setPropertiesAsMap(Map<String, String> properties) {
    try {
      this.properties = properties == null ? null : MAPPER.writeValueAsString(properties);
    } catch (JsonProcessingException ex) {
      throw new IllegalStateException(
          String.format("Failed to serialize json. properties %s", properties), ex);
    }
  }

  public String getInternalProperties() {
    return internalProperties != null ? internalProperties : EMPTY_MAP_STRING;
  }

  @JsonIgnore
  public Map<String, String> getInternalPropertiesAsMap() {
    if (this.internalProperties == null) {
      return new HashMap<>();
    }
    try {
      return MAPPER.readValue(this.internalProperties, new TypeReference<>() {});
    } catch (JsonProcessingException ex) {
      throw new IllegalStateException(
          String.format(
              "Failed to deserialize json. internalProperties %s", this.internalProperties),
          ex);
    }
  }

  /**
   * Set one single internal property
   *
   * @param propName name of the property
   * @param propValue value of that property
   */
  public void addInternalProperty(String propName, String propValue) {
    Map<String, String> props = this.getInternalPropertiesAsMap();
    props.put(propName, propValue);
    this.setInternalPropertiesAsMap(props);
  }

  public void setInternalProperties(String internalProperties) {
    this.internalProperties = internalProperties;
  }

  @JsonIgnore
  public void setInternalPropertiesAsMap(Map<String, String> internalProperties) {
    try {
      this.internalProperties =
          internalProperties == null ? null : MAPPER.writeValueAsString(internalProperties);
    } catch (JsonProcessingException ex) {
      throw new IllegalStateException(
          String.format("Failed to serialize json. internalProperties %s", internalProperties), ex);
    }
  }

  public int getGrantRecordsVersion() {
    return grantRecordsVersion;
  }

  public void setGrantRecordsVersion(int grantRecordsVersion) {
    this.grantRecordsVersion = grantRecordsVersion;
  }

  public static PolarisBaseEntity fromCore(
      PolarisEntityCore coreEntity, PolarisEntityType entityType, PolarisEntitySubType subType) {
    return new PolarisBaseEntity(
        coreEntity.getCatalogId(),
        coreEntity.getId(),
        entityType,
        subType,
        coreEntity.getParentId(),
        coreEntity.getName());
  }

  /**
   * Copy constructor
   *
   * @param entity entity to copy
   */
  public PolarisBaseEntity(PolarisBaseEntity entity) {
    super(
        entity.getCatalogId(),
        entity.getId(),
        entity.getParentId(),
        entity.getTypeCode(),
        entity.getName(),
        entity.getEntityVersion());
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

  /** Build the DTO for a new entity */
  public PolarisBaseEntity(
      long catalogId,
      long id,
      PolarisEntityType type,
      PolarisEntitySubType subType,
      long parentId,
      String name) {
    this(catalogId, id, type.getCode(), subType.getCode(), parentId, name);
  }

  /** Build the DTO for a new entity */
  protected PolarisBaseEntity(
      long catalogId, long id, int typeCode, int subTypeCode, long parentId, String name) {
    super(catalogId, id, parentId, typeCode, name, 1);
    this.subTypeCode = subTypeCode;
    this.createTimestamp = System.currentTimeMillis();
    this.dropTimestamp = 0;
    this.purgeTimestamp = 0;
    this.toPurgeTimestamp = 0;
    this.lastUpdateTimestamp = this.createTimestamp;
    this.properties = null;
    this.internalProperties = null;
    this.grantRecordsVersion = 1;
  }

  /** Build the DTO for a new entity */
  protected PolarisBaseEntity() {
    super();
  }

  /**
   * @return the subtype of this entity
   */
  public @JsonIgnore PolarisEntitySubType getSubType() {
    return PolarisEntitySubType.fromCode(this.subTypeCode);
  }

  /**
   * @return true if this entity has been dropped
   */
  public @JsonIgnore boolean isDropped() {
    return this.dropTimestamp != 0;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof PolarisBaseEntity)) {
      return false;
    }
    PolarisBaseEntity that = (PolarisBaseEntity) o;
    return subTypeCode == that.subTypeCode
        && createTimestamp == that.createTimestamp
        && dropTimestamp == that.dropTimestamp
        && purgeTimestamp == that.purgeTimestamp
        && toPurgeTimestamp == that.toPurgeTimestamp
        && lastUpdateTimestamp == that.lastUpdateTimestamp
        && grantRecordsVersion == that.grantRecordsVersion
        && Objects.equals(properties, that.properties)
        && Objects.equals(internalProperties, that.internalProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        catalogId,
        id,
        parentId,
        typeCode,
        name,
        entityVersion,
        subTypeCode,
        createTimestamp,
        dropTimestamp,
        purgeTimestamp,
        toPurgeTimestamp,
        lastUpdateTimestamp,
        properties,
        internalProperties,
        grantRecordsVersion);
  }

  @Override
  public String toString() {
    return "PolarisBaseEntity{"
        + super.toString()
        + ", subTypeCode="
        + subTypeCode
        + ", createTimestamp="
        + createTimestamp
        + ", dropTimestamp="
        + dropTimestamp
        + ", purgeTimestamp="
        + purgeTimestamp
        + ", toPurgeTimestamp="
        + toPurgeTimestamp
        + ", lastUpdateTimestamp="
        + lastUpdateTimestamp
        + ", grantRecordsVersion="
        + grantRecordsVersion
        + '}';
  }
}

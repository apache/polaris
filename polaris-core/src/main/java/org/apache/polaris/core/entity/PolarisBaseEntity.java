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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Base polaris entity representing all attributes of a Polaris Entity. This is used to exchange
 * full entity information between the client and the backend
 */
public class PolarisBaseEntity extends PolarisEntityCore {

  public static final String EMPTY_MAP_STRING = "{}";

  // to serialize/deserialize properties
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // the type of the entity when it was resolved
  protected final int subTypeCode;

  // timestamp when this entity was created
  protected final long createTimestamp;

  // when this entity was dropped. Null if was never dropped
  protected final long dropTimestamp;

  // when did we start purging this entity. When not null, un-drop is no longer possible
  protected final long purgeTimestamp;

  // when should we start purging this entity
  protected final long toPurgeTimestamp;

  // last time this entity was updated, only for troubleshooting
  protected final long lastUpdateTimestamp;

  // properties, serialized as a JSON string
  protected final String properties;

  // internal properties, serialized as a JSON string
  protected final String internalProperties;

  // current version for that entity, will be monotonically incremented
  protected final int grantRecordsVersion;

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

  /**
   * Creates a new instance of PolarisBaseEntity with the specified grant records version. This
   * method is used to update the grantRecordsVersion field while maintaining immutability of the
   * PolarisBaseEntity class.
   *
   * @param grantRecordsVersion The new grant records version to be set.
   * @return A new PolarisBaseEntity instance with the updated grant records version.
   */
  public PolarisBaseEntity withGrantRecordsVersion(int grantRecordsVersion) {
    return new Builder(this).grantRecordsVersion(grantRecordsVersion).build();
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

  @JsonIgnore
  public static String convertPropertiesToJson(Map<String, String> properties) {
    try {
      return properties == null ? null : MAPPER.writeValueAsString(properties);
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

  public int getGrantRecordsVersion() {
    return grantRecordsVersion;
  }

  public static class Builder extends PolarisEntityCore.Builder<PolarisBaseEntity, Builder> {
    private int subTypeCode;
    private long createTimestamp;
    private long dropTimestamp;
    private long purgeTimestamp;
    private long toPurgeTimestamp;
    private long lastUpdateTimestamp;
    private String properties;
    private String internalProperties;
    private int grantRecordsVersion;

    public Builder() {}

    public Builder subTypeCode(int subTypeCode) {
      this.subTypeCode = subTypeCode;
      return this;
    }

    public Builder createTimestamp(long createTimestamp) {
      this.createTimestamp = createTimestamp;
      return self();
    }

    public Builder dropTimestamp(long dropTimestamp) {
      this.dropTimestamp = dropTimestamp;
      return self();
    }

    public Builder purgeTimestamp(long purgeTimestamp) {
      this.purgeTimestamp = purgeTimestamp;
      return self();
    }

    public Builder toPurgeTimestamp(long toPurgeTimestamp) {
      this.toPurgeTimestamp = toPurgeTimestamp;
      return self();
    }

    public Builder lastUpdateTimestamp(long lastUpdateTimestamp) {
      this.lastUpdateTimestamp = lastUpdateTimestamp;
      return self();
    }

    public Builder propertiesAsMap(Map<String, String> properties) {
      this.properties = convertPropertiesToJson(properties);
      return self();
    }

    public Builder properties(String properties) {
      this.properties = properties;
      return self();
    }

    public Builder internalProperties(String internalProperties) {
      this.internalProperties = internalProperties;
      return self();
    }

    public Builder internalPropertiesAsMap(Map<String, String> internalProperties) {
      this.internalProperties = convertPropertiesToJson(internalProperties);
      return self();
    }

    public Builder grantRecordsVersion(int grantRecordsVersion) {
      this.grantRecordsVersion = grantRecordsVersion;
      return self();
    }

    public Builder(PolarisBaseEntity entity) {
      super.catalogId(entity.getCatalogId());
      super.id(entity.getId());
      super.parentId(entity.getParentId());
      super.typeCode(entity.getTypeCode());
      super.name(entity.getName());
      super.entityVersion(entity.getEntityVersion());
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

    @Override
    public PolarisBaseEntity build() {
      return new PolarisBaseEntity(this);
    }
  }

  private PolarisBaseEntity(Builder builder) {
    super(builder);
    this.subTypeCode = builder.subTypeCode;
    this.createTimestamp =
        builder.createTimestamp == 0L ? System.currentTimeMillis() : builder.createTimestamp;
    this.dropTimestamp = builder.dropTimestamp;
    this.purgeTimestamp = builder.purgeTimestamp;
    this.toPurgeTimestamp = builder.toPurgeTimestamp;
    this.lastUpdateTimestamp = builder.lastUpdateTimestamp;
    this.properties = builder.properties;
    this.internalProperties = builder.internalProperties;
    this.grantRecordsVersion = builder.grantRecordsVersion == 0 ? 1 : builder.grantRecordsVersion;
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
    super(
        new PolarisEntityCore.Builder<>()
            .catalogId(catalogId)
            .id(id)
            .parentId(parentId)
            .typeCode(typeCode)
            .name(name)
            .entityVersion(1));
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

  public PolarisBaseEntity(
      long catalogId,
      long id,
      int typeCode,
      long parentId,
      String name,
      int subTypeCode,
      long createTimestamp,
      long dropTimestamp,
      long purgeTimestamp,
      long lastUpdateTimestamp,
      String properties,
      String internalProperties,
      int grantRecordsVersion,
      int entityVersion) {
    super(
        new PolarisEntityCore.Builder<>()
            .catalogId(catalogId)
            .id(id)
            .parentId(parentId)
            .typeCode(typeCode)
            .name(name)
            .entityVersion(entityVersion == 0 ? 1 : entityVersion));
    this.subTypeCode = subTypeCode;
    this.createTimestamp = createTimestamp;
    this.dropTimestamp = dropTimestamp;
    this.purgeTimestamp = purgeTimestamp;
    this.toPurgeTimestamp = 0;
    this.lastUpdateTimestamp = lastUpdateTimestamp;
    this.properties = properties;
    this.internalProperties = internalProperties;
    this.grantRecordsVersion = grantRecordsVersion;
  }

  /** Build the DTO for a new entity */
  protected PolarisBaseEntity() {
    super(new PolarisBaseEntity.Builder());
    this.subTypeCode = 0;
    this.createTimestamp = 0L;
    this.dropTimestamp = 0L;
    this.purgeTimestamp = 0L;
    this.toPurgeTimestamp = 0L;
    this.lastUpdateTimestamp = 0L;
    this.properties = null;
    this.internalProperties = null;
    this.grantRecordsVersion = 0;
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

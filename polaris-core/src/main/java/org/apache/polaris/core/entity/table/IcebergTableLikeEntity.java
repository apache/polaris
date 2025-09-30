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
package org.apache.polaris.core.entity.table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * An entity type for {@link TableLikeEntity} instances that conform to iceberg semantics around
 * locations. This includes both Iceberg tables and Iceberg views.
 */
public class IcebergTableLikeEntity extends TableLikeEntity {
  // For applicable types, this key on the "internalProperties" map will return the location
  // of the internalProperties JSON file.
  public static final String METADATA_LOCATION_KEY = "metadata-location";

  public static final String USER_SPECIFIED_WRITE_DATA_LOCATION_KEY = "write.data.path";
  public static final String USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY = "write.metadata.path";

  public static final String LAST_ADMITTED_NOTIFICATION_TIMESTAMP_KEY =
      "last-notification-timestamp";

  public IcebergTableLikeEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
    PolarisEntitySubType subType = getSubType();
    Preconditions.checkState(
        subType == PolarisEntitySubType.ICEBERG_TABLE
            || subType == PolarisEntitySubType.ICEBERG_VIEW,
        "Invalid entity sub type: %s",
        subType);
  }

  public static @Nullable IcebergTableLikeEntity of(@Nullable PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new IcebergTableLikeEntity(sourceEntity);
    }
    return null;
  }

  @JsonIgnore
  public String getMetadataLocation() {
    return getInternalPropertiesAsMap().get(METADATA_LOCATION_KEY);
  }

  @JsonIgnore
  public Optional<Long> getLastAdmittedNotificationTimestamp() {
    return Optional.ofNullable(
            getInternalPropertiesAsMap().get(LAST_ADMITTED_NOTIFICATION_TIMESTAMP_KEY))
        .map(Long::parseLong);
  }

  @Override
  @JsonIgnore
  public String getBaseLocation() {
    return getPropertiesAsMap().get(PolarisEntityConstants.ENTITY_BASE_LOCATION);
  }

  public static class Builder extends PolarisEntity.BaseBuilder<IcebergTableLikeEntity, Builder> {

    public Builder(
        PolarisEntitySubType subType,
        TableIdentifier identifier,
        Map<String, String> properties,
        Map<String, String> internalProperties,
        String metadataLocation) {
      super();
      setType(PolarisEntityType.TABLE_LIKE);
      setSubType(subType);
      setProperties(properties);
      setInternalProperties(internalProperties);
      // order here matters. properties and internal properties must be set prior to the following
      // properties, which merely update the map, whereas the above calls replace the map entirely.
      setTableIdentifier(identifier);
      setMetadataLocation(metadataLocation);
    }

    public Builder(
        PolarisEntitySubType subType, TableIdentifier identifier, String metadataLocation) {
      super();
      setType(PolarisEntityType.TABLE_LIKE);
      setSubType(subType);
      setTableIdentifier(identifier);
      setMetadataLocation(metadataLocation);
    }

    public Builder(IcebergTableLikeEntity original) {
      super(original);
    }

    @Override
    public IcebergTableLikeEntity build() {
      return new IcebergTableLikeEntity(buildBase());
    }

    public Builder setTableIdentifier(TableIdentifier identifier) {
      Namespace namespace = identifier.namespace();
      setParentNamespace(namespace);
      setName(identifier.name());
      return this;
    }

    public Builder setParentNamespace(Namespace namespace) {
      if (namespace != null && !namespace.isEmpty()) {
        internalProperties.put(
            NamespaceEntity.PARENT_NAMESPACE_KEY, RESTUtil.encodeNamespace(namespace));
      }
      return this;
    }

    public Builder setBaseLocation(String location) {
      properties.put(PolarisEntityConstants.ENTITY_BASE_LOCATION, location);
      return this;
    }

    @Override
    public Builder setInternalProperties(@Nonnull Map<String, String> internalProperties) {
      // ensure we carry forward the parent namespace and metadata location if already set.
      // however, we allow for overriding them if explicitly specified in the provided map.
      Map<String, String> newInternalProperties = new HashMap<>();
      if (this.internalProperties.get(NamespaceEntity.PARENT_NAMESPACE_KEY) != null) {
        newInternalProperties.put(
            NamespaceEntity.PARENT_NAMESPACE_KEY,
            this.internalProperties.get(NamespaceEntity.PARENT_NAMESPACE_KEY));
      }
      if (this.internalProperties.get(METADATA_LOCATION_KEY) != null) {
        newInternalProperties.put(
            METADATA_LOCATION_KEY, this.internalProperties.get(METADATA_LOCATION_KEY));
      }
      if (this.internalProperties.get(LAST_ADMITTED_NOTIFICATION_TIMESTAMP_KEY) != null) {
        newInternalProperties.put(
            LAST_ADMITTED_NOTIFICATION_TIMESTAMP_KEY,
            this.internalProperties.get(LAST_ADMITTED_NOTIFICATION_TIMESTAMP_KEY));
      }
      newInternalProperties.putAll(internalProperties);
      return super.setInternalProperties(newInternalProperties);
    }

    public Builder setMetadataLocation(String location) {
      internalProperties.put(METADATA_LOCATION_KEY, location);
      return this;
    }

    public Builder setLastNotificationTimestamp(long timestamp) {
      internalProperties.put(LAST_ADMITTED_NOTIFICATION_TIMESTAMP_KEY, String.valueOf(timestamp));
      return this;
    }
  }
}

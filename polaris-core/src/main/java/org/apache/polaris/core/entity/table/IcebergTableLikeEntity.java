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
import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityType;

/**
 * An entity type for {@link TableLikeEntity} instances that conform to iceberg semantics around
 * locations. This includes both Iceberg tables and Iceberg views.
 */
public class IcebergTableLikeEntity extends TableLikeEntity {
  // For applicable types, this key on the "internalProperties" map will return the location
  // of the internalProperties JSON file.
  public static final String METADATA_LOCATION_KEY = "metadata-location";

  // For applicable types, this key on the "internalProperties" map will return the content of the
  // metadata.json file located at `METADATA_CACHE_LOCATION_KEY`
  public static final String METADATA_CACHE_CONTENT_KEY = "metadata-cache-content";

  // For applicable types, this key on the "internalProperties" map will return the location of the
  // `metadata.json` that is cached in `METADATA_CACHE_CONTENT_KEY`. This will often match the
  // current metadata location in `METADATA_LOCATION_KEY`; if it does not the cache is invalid
  public static final String METADATA_CACHE_LOCATION_KEY = "metadata-cache-location";

  public static final String USER_SPECIFIED_WRITE_DATA_LOCATION_KEY = "write.data.path";
  public static final String USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY = "write.metadata.path";

  public static final String LAST_ADMITTED_NOTIFICATION_TIMESTAMP_KEY =
      "last-notification-timestamp";

  public IcebergTableLikeEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
  }

  public static IcebergTableLikeEntity of(PolarisBaseEntity sourceEntity) {
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
  public String getMetadataCacheContent() {
    return getInternalPropertiesAsMap().get(METADATA_CACHE_CONTENT_KEY);
  }

  @JsonIgnore
  public String getMetadataCacheLocation() {
    return getInternalPropertiesAsMap().get(METADATA_CACHE_LOCATION_KEY);
  }

  @JsonIgnore
  public Optional<Long> getLastAdmittedNotificationTimestamp() {
    return Optional.ofNullable(
            getInternalPropertiesAsMap().get(LAST_ADMITTED_NOTIFICATION_TIMESTAMP_KEY))
        .map(Long::parseLong);
  }

  @JsonIgnore
  public String getBaseLocation() {
    return getPropertiesAsMap().get(PolarisEntityConstants.ENTITY_BASE_LOCATION);
  }

  public static class Builder extends PolarisEntity.BaseBuilder<IcebergTableLikeEntity, Builder> {
    public Builder(TableIdentifier identifier, String metadataLocation) {
      super();
      setType(PolarisEntityType.TABLE_LIKE);
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

    public Builder setMetadataLocation(String location) {
      internalProperties.put(METADATA_LOCATION_KEY, location);
      return this;
    }

    public Builder setMetadataContent(String location, String content) {
      internalProperties.put(METADATA_CACHE_LOCATION_KEY, location);
      internalProperties.put(METADATA_CACHE_CONTENT_KEY, content);
      return this;
    }

    public Builder setLastNotificationTimestamp(long timestamp) {
      internalProperties.put(LAST_ADMITTED_NOTIFICATION_TIMESTAMP_KEY, String.valueOf(timestamp));
      return this;
    }
  }
}

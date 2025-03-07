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
import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;

public class TableLikeEntity extends PolarisEntity {
  // For applicable types, this key on the "internalProperties" map will return the location
  // of the internalProperties JSON file.
  public static final String METADATA_LOCATION_KEY = "metadata-location";

  public static final String USER_SPECIFIED_WRITE_DATA_LOCATION_KEY = "write.data.path";
  public static final String USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY = "write.metadata.path";

  public static final String LAST_ADMITTED_NOTIFICATION_TIMESTAMP_KEY =
      "last-notification-timestamp";

  public TableLikeEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
  }

  public static TableLikeEntity of(PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new TableLikeEntity(sourceEntity);
    }
    return null;
  }

  @JsonIgnore
  public TableIdentifier getTableIdentifier() {
    Namespace parent = getParentNamespace();
    return TableIdentifier.of(parent, getName());
  }

  @JsonIgnore
  public Namespace getParentNamespace() {
    String encodedNamespace =
        getInternalPropertiesAsMap().get(NamespaceEntity.PARENT_NAMESPACE_KEY);
    if (encodedNamespace == null) {
      return Namespace.empty();
    }
    return RESTUtil.decodeNamespace(encodedNamespace);
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

  @JsonIgnore
  public String getBaseLocation() {
    return getPropertiesAsMap().get(PolarisEntityConstants.ENTITY_BASE_LOCATION);
  }

  public static class Builder extends PolarisEntity.BaseBuilder<TableLikeEntity, Builder> {
    public Builder(TableIdentifier identifier, String metadataLocation) {
      super();
      setType(PolarisEntityType.TABLE_LIKE);
      setTableIdentifier(identifier);
      setMetadataLocation(metadataLocation);
    }

    public Builder(TableLikeEntity original) {
      super(original);
    }

    @Override
    public TableLikeEntity build() {
      return new TableLikeEntity(buildBase());
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

    public Builder setLastNotificationTimestamp(long timestamp) {
      internalProperties.put(LAST_ADMITTED_NOTIFICATION_TIMESTAMP_KEY, String.valueOf(timestamp));
      return this;
    }
  }
}

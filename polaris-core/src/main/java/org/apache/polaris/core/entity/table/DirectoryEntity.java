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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.jspecify.annotations.Nullable;

/**
 * A {@link TableLikeEntity} implementation for directories. Directories represent folders on an
 * object store containing unstructured data (images, videos, documents, etc.).
 */
public class DirectoryEntity extends TableLikeEntity {

  public static final String BASE_LOCATION_KEY = "base_location";

  public static final String FILTER_INCLUDE_KEY = "filter-include";
  public static final String FILTER_EXCLUDE_KEY = "filter-exclude";
  public static final String SCAN_SCHEDULE_KEY = "scan-schedule";

  public DirectoryEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
    Preconditions.checkState(
        getSubType() == PolarisEntitySubType.DIRECTORY,
        "Invalid entity sub type: %s",
        getSubType());
  }

  public static @Nullable DirectoryEntity of(@Nullable PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new DirectoryEntity(sourceEntity);
    }
    return null;
  }

  @JsonIgnore
  public String getFilterInclude() {
    return getInternalPropertiesAsMap().get(DirectoryEntity.FILTER_INCLUDE_KEY);
  }

  @JsonIgnore
  public String getFilterExclude() {
    return getInternalPropertiesAsMap().get(DirectoryEntity.FILTER_EXCLUDE_KEY);
  }

  @JsonIgnore
  public String getScanSchedule() {
    return getInternalPropertiesAsMap().get(DirectoryEntity.SCAN_SCHEDULE_KEY);
  }

  @Override
  @JsonIgnore
  public String getBaseLocation() {
    return getInternalPropertiesAsMap().get(DirectoryEntity.BASE_LOCATION_KEY);
  }

  public static class Builder
      extends PolarisEntity.BaseBuilder<DirectoryEntity, DirectoryEntity.Builder> {
    public Builder(TableIdentifier tableIdentifier, String baseLocation) {
      super();
      setType(PolarisEntityType.TABLE_LIKE);
      setSubType(PolarisEntitySubType.DIRECTORY);
      setTableIdentifier(tableIdentifier);
      setBaseLocation(baseLocation);
    }

    public DirectoryEntity.Builder setBaseLocation(String baseLocation) {
      internalProperties.put(DirectoryEntity.BASE_LOCATION_KEY, baseLocation);
      return this;
    }

    public DirectoryEntity.Builder setFilterInclude(String filterInclude) {
      if (filterInclude != null) {
        internalProperties.put(DirectoryEntity.FILTER_INCLUDE_KEY, filterInclude);
      }
      return this;
    }

    public DirectoryEntity.Builder setFilterExclude(String filterExclude) {
      if (filterExclude != null) {
        internalProperties.put(DirectoryEntity.FILTER_EXCLUDE_KEY, filterExclude);
      }
      return this;
    }

    public DirectoryEntity.Builder setScanSchedule(String scanSchedule) {
      if (scanSchedule != null) {
        internalProperties.put(DirectoryEntity.SCAN_SCHEDULE_KEY, scanSchedule);
      }
      return this;
    }

    public DirectoryEntity.Builder setTableIdentifier(TableIdentifier identifier) {
      Namespace namespace = identifier.namespace();
      setParentNamespace(namespace);
      setName(identifier.name());
      return this;
    }

    public DirectoryEntity.Builder setParentNamespace(Namespace namespace) {
      if (namespace != null && !namespace.isEmpty()) {
        internalProperties.put(
            NamespaceEntity.PARENT_NAMESPACE_KEY, RESTUtil.encodeNamespace(namespace));
      }
      return this;
    }

    @Override
    public DirectoryEntity build() {
      return new DirectoryEntity(buildBase());
    }
  }
}

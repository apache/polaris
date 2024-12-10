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

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;

public class ForeignTableEntity extends TableLikeEntity {

  /**
   * A string which describes the underlying format/source. For example "delta", "hudi",
   * "cassandra". Polaris will not validate or use this except when deciding whether a table should
   * be passed through the TableConversionService.
   */
  public static final String FOREIGN_SOURCE_KEY = "_source";

  public ForeignTableEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
  }

  public String getSource() {
    return getPropertiesAsMap().get(FOREIGN_SOURCE_KEY);
  }

  public static class Builder
      extends PolarisEntity.BaseBuilder<ForeignTableEntity, ForeignTableEntity.Builder> {
    public Builder(TableIdentifier identifier, String metadataLocation) {
      super();
      setType(PolarisEntityType.FOREIGN_TABLE);
      setTableIdentifier(identifier);
      setMetadataLocation(metadataLocation);
    }

    public Builder(ForeignTableEntity original) {
      super(original);
    }

    @Override
    public ForeignTableEntity build() {
      return new ForeignTableEntity(buildBase());
    }

    public ForeignTableEntity.Builder setTableIdentifier(TableIdentifier identifier) {
      Namespace namespace = identifier.namespace();
      setParentNamespace(namespace);
      setName(identifier.name());
      return this;
    }

    public ForeignTableEntity.Builder setParentNamespace(Namespace namespace) {
      if (namespace != null && !namespace.isEmpty()) {
        internalProperties.put(
            NamespaceEntity.PARENT_NAMESPACE_KEY, RESTUtil.encodeNamespace(namespace));
      }
      return this;
    }

    public ForeignTableEntity.Builder setBaseLocation(String location) {
      properties.put(PolarisEntityConstants.ENTITY_BASE_LOCATION, location);
      return this;
    }

    public ForeignTableEntity.Builder setSource(String source) {
      properties.put(FOREIGN_SOURCE_KEY, source);
      return this;
    }

    public ForeignTableEntity.Builder setMetadataLocation(String location) {
      internalProperties.put(METADATA_LOCATION_KEY, location);
      return this;
    }

    public ForeignTableEntity.Builder setLastNotificationTimestamp(long timestamp) {
      internalProperties.put(LAST_ADMITTED_NOTIFICATION_TIMESTAMP_KEY, String.valueOf(timestamp));
      return this;
    }
  }
}

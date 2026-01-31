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
package org.apache.polaris.service.catalog.io;

import java.util.Map;
import org.apache.iceberg.TableMetadata;

/**
 * Strategy interface for merging table-level storage properties with catalog-level configuration.
 * Implementations handle specific storage types (S3, Azure, GCS) and determine precedence rules.
 */
public interface TableStorageConfigurationProvider {

  /**
   * Determines if this provider can handle the given table's storage configuration. This is used
   * for cross-cloud "hot swapping" where a table might use different storage than the catalog
   * default.
   *
   * @param tableMetadata the table metadata containing potential storage properties
   * @return true if this provider should process the table's storage config
   */
  boolean canHandle(TableMetadata tableMetadata);

  /**
   * Determines if this provider can handle the given table properties. This is used for cross-cloud
   * "hot swapping" where a table might use different storage than the catalog default.
   *
   * @param tableProperties the table properties map containing potential storage properties
   * @return true if this provider should process the table's storage config
   */
  default boolean canHandle(Map<String, String> tableProperties) {
    // Default implementation delegates to property-based logic
    // Subclasses can override for efficiency if needed
    return tableProperties != null && !tableProperties.isEmpty();
  }

  /**
   * Merges table-level storage properties with catalog-level configuration. Table properties take
   * precedence over catalog properties.
   *
   * @param catalogConfig the base configuration from the catalog storage integration
   * @param tableMetadata the table metadata containing potential override properties
   * @return merged configuration map with table properties overriding catalog properties
   */
  Map<String, String> mergeConfigurations(
      Map<String, String> catalogConfig, TableMetadata tableMetadata);

  /**
   * Merges table-level storage properties with catalog-level configuration. Table properties take
   * precedence over catalog properties.
   *
   * @param catalogConfig the base configuration from the catalog storage integration
   * @param tableProperties the table properties map containing potential override properties
   * @return merged configuration map with table properties overriding catalog properties
   */
  default Map<String, String> mergeConfigurations(
      Map<String, String> catalogConfig, Map<String, String> tableProperties) {
    // Default implementation just returns catalog config
    // Subclasses must override to provide actual merge logic
    return catalogConfig;
  }

  /**
   * Returns the storage type this provider handles (e.g., "s3", "azure", "gcs").
   *
   * @return the storage type identifier
   */
  String getStorageType();
}

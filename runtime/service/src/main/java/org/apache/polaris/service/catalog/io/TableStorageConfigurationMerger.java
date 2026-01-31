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

import jakarta.enterprise.context.ApplicationScoped;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates table-level storage configuration overrides. This class merges catalog-level storage
 * configuration with table-level properties, supporting cross-cloud scenarios where a table may use
 * different storage than the catalog.
 */
@ApplicationScoped
public class TableStorageConfigurationMerger {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TableStorageConfigurationMerger.class);

  private final List<TableStorageConfigurationProvider> providers;

  public TableStorageConfigurationMerger() {
    // Initialize all supported providers
    this.providers =
        List.of(
            new S3TableStorageConfigurationProvider(),
            new AzureTableStorageConfigurationProvider(),
            new GcsTableStorageConfigurationProvider());
  }

  /**
   * Merges catalog-level storage configuration with table-level properties. Table properties always
   * take precedence over catalog properties. Supports cross-cloud scenarios where table storage
   * type differs from catalog default.
   *
   * @param catalogConfig the base configuration from catalog storage integration
   * @param tableMetadata the table metadata containing potential override properties
   * @return merged configuration with table properties taking precedence
   */
  public Map<String, String> mergeConfigurations(
      Map<String, String> catalogConfig, TableMetadata tableMetadata) {

    if (tableMetadata == null
        || tableMetadata.properties() == null
        || tableMetadata.properties().isEmpty()) {
      LOGGER.debug("No table properties to merge, returning catalog config");
      return new HashMap<>(catalogConfig);
    }

    return mergeConfigurations(catalogConfig, tableMetadata.properties());
  }

  /**
   * Merges catalog-level storage configuration with table-level properties. Table properties always
   * take precedence over catalog properties. Supports cross-cloud scenarios where table storage
   * type differs from catalog default.
   *
   * @param catalogConfig the base configuration from catalog storage integration
   * @param tableProperties the table properties map containing potential override properties
   * @return merged configuration with table properties taking precedence
   */
  public Map<String, String> mergeConfigurations(
      Map<String, String> catalogConfig, Map<String, String> tableProperties) {

    if (tableProperties == null || tableProperties.isEmpty()) {
      LOGGER.debug("No table properties to merge, returning catalog config");
      return new HashMap<>(catalogConfig);
    }

    // Find the appropriate provider based on table properties
    // This supports cross-cloud "hot swapping" - a table can declare Azure properties
    // even if the catalog is configured for S3
    TableStorageConfigurationProvider selectedProvider = null;
    for (TableStorageConfigurationProvider provider : providers) {
      if (provider.canHandle(tableProperties)) {
        selectedProvider = provider;
        LOGGER.debug("Selected {} provider for table properties", provider.getStorageType());
        break;
      }
    }

    if (selectedProvider != null) {
      Map<String, String> merged =
          selectedProvider.mergeConfigurations(catalogConfig, tableProperties);
      LOGGER.debug(
          "Merged table properties into catalog config. Original keys: {}, Final keys: {}",
          catalogConfig.keySet(),
          merged.keySet());
      return merged;
    }

    // No table-level storage properties found, return catalog config as-is
    LOGGER.debug("No storage-specific properties found in table, using catalog config");
    return new HashMap<>(catalogConfig);
  }

  /**
   * Determines if the table has any storage configuration properties that would override catalog
   * configuration.
   *
   * @param tableMetadata the table metadata to check
   * @return true if table has storage override properties
   */
  public boolean hasTableStorageProperties(TableMetadata tableMetadata) {
    if (tableMetadata == null || tableMetadata.properties() == null) {
      return false;
    }

    return hasTableStorageProperties(tableMetadata.properties());
  }

  /**
   * Determines if the table properties map has any storage configuration properties that would
   * override catalog configuration.
   *
   * @param tableProperties the table properties map to check
   * @return true if table has storage override properties
   */
  public boolean hasTableStorageProperties(Map<String, String> tableProperties) {
    if (tableProperties == null || tableProperties.isEmpty()) {
      return false;
    }

    return providers.stream().anyMatch(provider -> provider.canHandle(tableProperties));
  }
}

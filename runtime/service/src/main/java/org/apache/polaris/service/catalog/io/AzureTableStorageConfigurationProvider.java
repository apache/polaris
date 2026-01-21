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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.TableMetadata;

/**
 * Provider for Azure storage configuration merging. Handles Azure ADLS/Blob-specific properties and
 * allows table-level properties to override catalog config.
 */
public class AzureTableStorageConfigurationProvider implements TableStorageConfigurationProvider {

  // Azure property keys that can be overridden at table level
  private static final Set<String> AZURE_PROPERTY_KEYS =
      Set.of(
          "adls.auth.shared-key.account.name",
          "adls.auth.shared-key.account.key",
          "adls.sas-token",
          "adls.connection-string",
          "adls.endpoint",
          "azure.tenant-id",
          "azure.client-id",
          "azure.client-secret",
          "azure.msi-endpoint",
          "azure.use-managed-identity",
          "abfs.auth.shared-key.account.name",
          "abfs.auth.shared-key.account.key");

  @Override
  public boolean canHandle(TableMetadata tableMetadata) {
    if (tableMetadata == null || tableMetadata.properties() == null) {
      return false;
    }

    // Check if any Azure-specific properties exist in table metadata
    return tableMetadata.properties().keySet().stream()
        .anyMatch(
            key -> key.startsWith("adls.") || key.startsWith("abfs.") || key.startsWith("azure."));
  }

  @Override
  public boolean canHandle(Map<String, String> tableProperties) {
    if (tableProperties == null || tableProperties.isEmpty()) {
      return false;
    }

    // Check if any Azure-specific properties exist
    return tableProperties.keySet().stream()
        .anyMatch(
            key -> key.startsWith("adls.") || key.startsWith("abfs.") || key.startsWith("azure."));
  }

  @Override
  public Map<String, String> mergeConfigurations(
      Map<String, String> catalogConfig, TableMetadata tableMetadata) {
    if (tableMetadata == null || tableMetadata.properties() == null) {
      return new HashMap<>(catalogConfig);
    }

    return mergeConfigurations(catalogConfig, tableMetadata.properties());
  }

  @Override
  public Map<String, String> mergeConfigurations(
      Map<String, String> catalogConfig, Map<String, String> tableProperties) {
    Map<String, String> merged = new HashMap<>(catalogConfig);

    if (tableProperties == null || tableProperties.isEmpty()) {
      return merged;
    }

    // Override with table-level Azure properties
    tableProperties.entrySet().stream()
        .filter(entry -> AZURE_PROPERTY_KEYS.contains(entry.getKey()))
        .forEach(entry -> merged.put(entry.getKey(), entry.getValue()));

    return merged;
  }

  @Override
  public String getStorageType() {
    return "azure";
  }
}

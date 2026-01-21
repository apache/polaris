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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TableStorageConfigurationMergerTest {

  private final TableStorageConfigurationMerger merger = new TableStorageConfigurationMerger();

  private TableMetadata createTableMetadata(Map<String, String> properties) {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    return TableMetadata.newTableMetadata(
        schema,
        PartitionSpec.unpartitioned(),
        SortOrder.unsorted(),
        "s3://bucket/table",
        properties);
  }

  @Test
  public void testS3TablePropertiesOverrideCatalogConfig() {
    // Catalog config has default S3 credentials
    Map<String, String> catalogConfig = new HashMap<>();
    catalogConfig.put("s3.access-key-id", "catalog-access-key");
    catalogConfig.put("s3.secret-access-key", "catalog-secret-key");
    catalogConfig.put("s3.endpoint", "https://catalog.s3.amazonaws.com");
    catalogConfig.put("s3.region", "us-west-2");

    // Table properties override with different credentials
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("s3.access-key-id", "table-access-key");
    tableProperties.put("s3.secret-access-key", "table-secret-key");
    tableProperties.put("s3.endpoint", "https://table.s3.amazonaws.com");

    TableMetadata tableMetadata = createTableMetadata(tableProperties);

    Map<String, String> result = merger.mergeConfigurations(catalogConfig, tableMetadata);

    // Table properties should override catalog properties
    assertThat(result)
        .contains(
            entry("s3.access-key-id", "table-access-key"),
            entry("s3.secret-access-key", "table-secret-key"),
            entry("s3.endpoint", "https://table.s3.amazonaws.com"))
        // Catalog property without table override should remain
        .contains(entry("s3.region", "us-west-2"));
  }

  @Test
  public void testTablePropertiesOnlyFallbackToCatalog() {
    // Catalog config has credentials
    Map<String, String> catalogConfig = new HashMap<>();
    catalogConfig.put("s3.access-key-id", "catalog-access-key");
    catalogConfig.put("s3.secret-access-key", "catalog-secret-key");
    catalogConfig.put("s3.region", "us-east-1");

    // Table has NO storage properties
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("write.format.default", "parquet"); // Non-storage property

    TableMetadata tableMetadata = createTableMetadata(tableProperties);

    Map<String, String> result = merger.mergeConfigurations(catalogConfig, tableMetadata);

    // Should return catalog config unchanged
    assertThat(result).isEqualTo(catalogConfig);
  }

  @Test
  public void testEmptyCatalogConfigWithTableProperties() {
    // Catalog has NO credentials (possibly vending mode or misconfigured)
    Map<String, String> catalogConfig = new HashMap<>();

    // Table provides its own credentials
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("s3.access-key-id", "table-access-key");
    tableProperties.put("s3.secret-access-key", "table-secret-key");
    tableProperties.put("s3.region", "eu-west-1");

    TableMetadata tableMetadata = createTableMetadata(tableProperties);

    Map<String, String> result = merger.mergeConfigurations(catalogConfig, tableMetadata);

    // Should have table properties
    assertThat(result)
        .contains(
            entry("s3.access-key-id", "table-access-key"),
            entry("s3.secret-access-key", "table-secret-key"),
            entry("s3.region", "eu-west-1"));
  }

  @Test
  public void testCrossCloudSwapS3CatalogToAzureTable() {
    // Catalog configured for S3
    Map<String, String> catalogConfig = new HashMap<>();
    catalogConfig.put("s3.access-key-id", "catalog-s3-key");
    catalogConfig.put("s3.secret-access-key", "catalog-s3-secret");

    // Table has Azure properties (cross-cloud scenario)
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("adls.auth.shared-key.account.name", "azure-account");
    tableProperties.put("adls.auth.shared-key.account.key", "azure-key");

    TableMetadata tableMetadata = createTableMetadata(tableProperties);

    Map<String, String> result = merger.mergeConfigurations(catalogConfig, tableMetadata);

    // Result should include Azure properties from table
    assertThat(result)
        .contains(
            entry("adls.auth.shared-key.account.name", "azure-account"),
            entry("adls.auth.shared-key.account.key", "azure-key"))
        // Original S3 properties should still be there (merger doesn't remove them)
        .contains(
            entry("s3.access-key-id", "catalog-s3-key"),
            entry("s3.secret-access-key", "catalog-s3-secret"));
  }

  @Test
  public void testAzureTablePropertiesOverride() {
    // Catalog configured for Azure
    Map<String, String> catalogConfig = new HashMap<>();
    catalogConfig.put("adls.auth.shared-key.account.name", "catalog-account");
    catalogConfig.put("adls.auth.shared-key.account.key", "catalog-key");
    catalogConfig.put("adls.endpoint", "https://catalog.dfs.core.windows.net");

    // Table overrides Azure properties
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("adls.auth.shared-key.account.name", "table-account");
    tableProperties.put("adls.auth.shared-key.account.key", "table-key");

    TableMetadata tableMetadata = createTableMetadata(tableProperties);

    Map<String, String> result = merger.mergeConfigurations(catalogConfig, tableMetadata);

    assertThat(result)
        .contains(
            entry("adls.auth.shared-key.account.name", "table-account"),
            entry("adls.auth.shared-key.account.key", "table-key"))
        .contains(entry("adls.endpoint", "https://catalog.dfs.core.windows.net"));
  }

  @Test
  public void testGcsTablePropertiesOverride() {
    // Catalog configured for GCS
    Map<String, String> catalogConfig = new HashMap<>();
    catalogConfig.put("gcs.project-id", "catalog-project");
    catalogConfig.put("gcs.oauth2.token", "catalog-token");

    // Table overrides GCS properties
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("gcs.project-id", "table-project");
    tableProperties.put("gcs.oauth2.token", "table-token");

    TableMetadata tableMetadata = createTableMetadata(tableProperties);

    Map<String, String> result = merger.mergeConfigurations(catalogConfig, tableMetadata);

    assertThat(result)
        .contains(
            entry("gcs.project-id", "table-project"), entry("gcs.oauth2.token", "table-token"));
  }

  @Test
  public void testPartialOverride() {
    // Catalog has complete config
    Map<String, String> catalogConfig = new HashMap<>();
    catalogConfig.put("s3.access-key-id", "catalog-key");
    catalogConfig.put("s3.secret-access-key", "catalog-secret");
    catalogConfig.put("s3.session-token", "catalog-token");
    catalogConfig.put("s3.region", "us-west-2");
    catalogConfig.put("s3.endpoint", "https://catalog.endpoint.com");

    // Table only overrides some properties
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("s3.access-key-id", "table-key");
    tableProperties.put("s3.secret-access-key", "table-secret");
    // No session-token, region, or endpoint override

    TableMetadata tableMetadata = createTableMetadata(tableProperties);

    Map<String, String> result = merger.mergeConfigurations(catalogConfig, tableMetadata);

    // Overridden properties use table values
    assertThat(result)
        .contains(
            entry("s3.access-key-id", "table-key"), entry("s3.secret-access-key", "table-secret"))
        // Non-overridden properties use catalog values
        .contains(
            entry("s3.session-token", "catalog-token"),
            entry("s3.region", "us-west-2"),
            entry("s3.endpoint", "https://catalog.endpoint.com"));
  }

  @Test
  public void testHasTableStoragePropertiesS3() {
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("s3.access-key-id", "key");

    TableMetadata tableMetadata = createTableMetadata(tableProperties);

    assertThat(merger.hasTableStorageProperties(tableMetadata)).isTrue();
  }

  @Test
  public void testHasTableStoragePropertiesAzure() {
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("adls.auth.shared-key.account.name", "account");

    TableMetadata tableMetadata = createTableMetadata(tableProperties);

    assertThat(merger.hasTableStorageProperties(tableMetadata)).isTrue();
  }

  @Test
  public void testHasTableStoragePropertiesGcs() {
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("gcs.project-id", "project");

    TableMetadata tableMetadata = createTableMetadata(tableProperties);

    assertThat(merger.hasTableStorageProperties(tableMetadata)).isTrue();
  }

  @Test
  public void testHasNoTableStorageProperties() {
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("write.format.default", "parquet");
    tableProperties.put("read.split.target-size", "134217728");

    TableMetadata tableMetadata = createTableMetadata(tableProperties);

    assertThat(merger.hasTableStorageProperties(tableMetadata)).isFalse();
  }

  @Test
  public void testNullTableMetadata() {
    Map<String, String> catalogConfig = new HashMap<>();
    catalogConfig.put("s3.access-key-id", "catalog-key");

    Map<String, String> result = merger.mergeConfigurations(catalogConfig, (TableMetadata) null);

    assertThat(result).isEqualTo(catalogConfig);
    assertThat(merger.hasTableStorageProperties((TableMetadata) null)).isFalse();
  }

  @Test
  public void testEmptyProperties() {
    Map<String, String> catalogConfig = new HashMap<>();
    catalogConfig.put("s3.access-key-id", "catalog-key");

    Map<String, String> tableProperties = new HashMap<>();
    TableMetadata tableMetadata = createTableMetadata(tableProperties);

    Map<String, String> result = merger.mergeConfigurations(catalogConfig, tableMetadata);

    assertThat(result).isEqualTo(catalogConfig);
  }

  @Test
  public void testSessionTokenOverride() {
    Map<String, String> catalogConfig = new HashMap<>();
    catalogConfig.put("s3.access-key-id", "catalog-key");
    catalogConfig.put("s3.secret-access-key", "catalog-secret");
    catalogConfig.put("s3.session-token", "catalog-session-token");

    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("s3.session-token", "table-session-token");

    TableMetadata tableMetadata = createTableMetadata(tableProperties);

    Map<String, String> result = merger.mergeConfigurations(catalogConfig, tableMetadata);

    assertThat(result).contains(entry("s3.session-token", "table-session-token"));
  }

  @Test
  public void testClientRegionOverride() {
    Map<String, String> catalogConfig = new HashMap<>();
    catalogConfig.put("client.region", "us-west-1");

    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("client.region", "eu-central-1");

    TableMetadata tableMetadata = createTableMetadata(tableProperties);

    Map<String, String> result = merger.mergeConfigurations(catalogConfig, tableMetadata);

    assertThat(result).contains(entry("client.region", "eu-central-1"));
  }
}

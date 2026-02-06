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

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Integration tests demonstrating table-level storage configuration override scenarios. These tests
 * verify the provider implementations work correctly for each cloud type.
 */
public class TableStorageConfigurationProviderTest {

  private Schema createSchema() {
    return new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  }

  @Test
  public void testS3ProviderCanHandleS3Properties() {
    S3TableStorageConfigurationProvider provider = new S3TableStorageConfigurationProvider();

    Map<String, String> s3Properties = new HashMap<>();
    s3Properties.put("s3.access-key-id", "key");

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            createSchema(),
            PartitionSpec.unpartitioned(),
            org.apache.iceberg.SortOrder.unsorted(),
            "s3://bucket/table",
            s3Properties);

    assertThat(provider.canHandle(metadata)).isTrue();
    assertThat(provider.getStorageType()).isEqualTo("s3");
  }

  @Test
  public void testS3ProviderCannotHandleAzureProperties() {
    S3TableStorageConfigurationProvider provider = new S3TableStorageConfigurationProvider();

    Map<String, String> azureProperties = new HashMap<>();
    azureProperties.put("adls.auth.shared-key.account.name", "account");

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            createSchema(),
            PartitionSpec.unpartitioned(),
            org.apache.iceberg.SortOrder.unsorted(),
            "abfs://container@account.dfs.core.windows.net/table",
            azureProperties);

    assertThat(provider.canHandle(metadata)).isFalse();
  }

  @Test
  public void testAzureProviderCanHandleAzureProperties() {
    AzureTableStorageConfigurationProvider provider = new AzureTableStorageConfigurationProvider();

    Map<String, String> azureProperties = new HashMap<>();
    azureProperties.put("adls.auth.shared-key.account.name", "account");

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            createSchema(),
            PartitionSpec.unpartitioned(),
            org.apache.iceberg.SortOrder.unsorted(),
            "abfs://container@account.dfs.core.windows.net/table",
            azureProperties);

    assertThat(provider.canHandle(metadata)).isTrue();
    assertThat(provider.getStorageType()).isEqualTo("azure");
  }

  @Test
  public void testAzureProviderCannotHandleGcsProperties() {
    AzureTableStorageConfigurationProvider provider = new AzureTableStorageConfigurationProvider();

    Map<String, String> gcsProperties = new HashMap<>();
    gcsProperties.put("gcs.project-id", "project");

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            createSchema(),
            PartitionSpec.unpartitioned(),
            org.apache.iceberg.SortOrder.unsorted(),
            "gs://bucket/table",
            gcsProperties);

    assertThat(provider.canHandle(metadata)).isFalse();
  }

  @Test
  public void testGcsProviderCanHandleGcsProperties() {
    GcsTableStorageConfigurationProvider provider = new GcsTableStorageConfigurationProvider();

    Map<String, String> gcsProperties = new HashMap<>();
    gcsProperties.put("gcs.project-id", "project");

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            createSchema(),
            PartitionSpec.unpartitioned(),
            org.apache.iceberg.SortOrder.unsorted(),
            "gs://bucket/table",
            gcsProperties);

    assertThat(provider.canHandle(metadata)).isTrue();
    assertThat(provider.getStorageType()).isEqualTo("gcs");
  }

  @Test
  public void testS3ProviderMergesAllS3Properties() {
    S3TableStorageConfigurationProvider provider = new S3TableStorageConfigurationProvider();

    Map<String, String> catalogConfig = new HashMap<>();
    catalogConfig.put("s3.access-key-id", "catalog-key");
    catalogConfig.put("s3.region", "us-west-1");

    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("s3.access-key-id", "table-key");
    tableProperties.put("s3.secret-access-key", "table-secret");
    tableProperties.put("s3.endpoint", "https://custom.endpoint.com");
    tableProperties.put("client.region", "eu-west-1");

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            createSchema(),
            PartitionSpec.unpartitioned(),
            org.apache.iceberg.SortOrder.unsorted(),
            "s3://bucket/table",
            tableProperties);

    Map<String, String> merged = provider.mergeConfigurations(catalogConfig, metadata);

    assertThat(merged)
        .containsEntry("s3.access-key-id", "table-key")
        .containsEntry("s3.secret-access-key", "table-secret")
        .containsEntry("s3.endpoint", "https://custom.endpoint.com")
        .containsEntry("client.region", "eu-west-1")
        .containsEntry("s3.region", "us-west-1"); // From catalog, not overridden
  }

  @Test
  public void testAzureProviderMergesAllAzureProperties() {
    AzureTableStorageConfigurationProvider provider = new AzureTableStorageConfigurationProvider();

    Map<String, String> catalogConfig = new HashMap<>();
    catalogConfig.put("adls.auth.shared-key.account.name", "catalog-account");

    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("adls.auth.shared-key.account.name", "table-account");
    tableProperties.put("adls.auth.shared-key.account.key", "table-key");
    tableProperties.put("azure.tenant-id", "tenant-id");

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            createSchema(),
            PartitionSpec.unpartitioned(),
            org.apache.iceberg.SortOrder.unsorted(),
            "abfs://container@account.dfs.core.windows.net/table",
            tableProperties);

    Map<String, String> merged = provider.mergeConfigurations(catalogConfig, metadata);

    assertThat(merged)
        .containsEntry("adls.auth.shared-key.account.name", "table-account")
        .containsEntry("adls.auth.shared-key.account.key", "table-key")
        .containsEntry("azure.tenant-id", "tenant-id");
  }

  @Test
  public void testGcsProviderMergesAllGcsProperties() {
    GcsTableStorageConfigurationProvider provider = new GcsTableStorageConfigurationProvider();

    Map<String, String> catalogConfig = new HashMap<>();
    catalogConfig.put("gcs.project-id", "catalog-project");

    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("gcs.project-id", "table-project");
    tableProperties.put("gcs.oauth2.token", "token");
    tableProperties.put("gcs.service.host", "https://custom.gcs.com");

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            createSchema(),
            PartitionSpec.unpartitioned(),
            org.apache.iceberg.SortOrder.unsorted(),
            "gs://bucket/table",
            tableProperties);

    Map<String, String> merged = provider.mergeConfigurations(catalogConfig, metadata);

    assertThat(merged)
        .containsEntry("gcs.project-id", "table-project")
        .containsEntry("gcs.oauth2.token", "token")
        .containsEntry("gcs.service.host", "https://custom.gcs.com");
  }

  @Test
  public void testProviderIgnoresNonStorageProperties() {
    S3TableStorageConfigurationProvider provider = new S3TableStorageConfigurationProvider();

    Map<String, String> catalogConfig = new HashMap<>();
    catalogConfig.put("s3.access-key-id", "catalog-key");

    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("write.format.default", "parquet");
    tableProperties.put("read.split.target-size", "134217728");
    tableProperties.put("s3.access-key-id", "table-key");

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            createSchema(),
            PartitionSpec.unpartitioned(),
            org.apache.iceberg.SortOrder.unsorted(),
            "s3://bucket/table",
            tableProperties);

    Map<String, String> merged = provider.mergeConfigurations(catalogConfig, metadata);

    // Only S3-specific properties should be merged
    assertThat(merged)
        .containsEntry("s3.access-key-id", "table-key")
        .doesNotContainKeys("write.format.default", "read.split.target-size");
  }

  @Test
  public void testProviderHandlesNullTableMetadata() {
    S3TableStorageConfigurationProvider provider = new S3TableStorageConfigurationProvider();

    Map<String, String> catalogConfig = new HashMap<>();
    catalogConfig.put("s3.access-key-id", "catalog-key");

    Map<String, String> merged = provider.mergeConfigurations(catalogConfig, (TableMetadata) null);

    assertThat(merged).isEqualTo(catalogConfig);
    assertThat(provider.canHandle((TableMetadata) null)).isFalse();
  }
}

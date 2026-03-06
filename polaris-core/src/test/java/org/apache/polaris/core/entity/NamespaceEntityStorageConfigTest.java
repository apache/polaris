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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.catalog.Namespace;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for storage configuration support in NamespaceEntity.
 *
 * <p>Tests storage config getter method for namespace entities.
 */
public class NamespaceEntityStorageConfigTest {

  /** Test serialization round-trip for Namespace entity with storage config. */
  @Test
  public void testNamespaceEntityStorageConfigRoundTrip() {
    // Create a storage config
    PolarisStorageConfigurationInfo originalConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://my-bucket/warehouse/namespace1")
            .roleARN("arn:aws:iam::123456789012:role/namespace-role")
            .region("us-east-1")
            .build();

    // Create a Namespace entity using the Builder with internal properties
    NamespaceEntity entity =
        new NamespaceEntity.Builder(Namespace.of("namespace1"))
            .setCatalogId(1L)
            .setId(10L)
            .setParentId(1L)
            .setBaseLocation("s3://my-bucket/warehouse/namespace1")
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(),
                originalConfig.serialize())
            .build();

    // Verify serialization happened (internal property should have JSON)
    String storedJson =
        entity
            .getInternalPropertiesAsMap()
            .get(PolarisEntityConstants.getStorageConfigInfoPropertyName());
    assertThat(storedJson).isNotNull();
    assertThat(storedJson).isNotEmpty();

    // Verify retrieval and deserialization
    PolarisStorageConfigurationInfo retrievedConfig = entity.getStorageConfigurationInfo();
    assertThat(retrievedConfig).isNotNull();
    assertThat(retrievedConfig.serialize()).isEqualTo(originalConfig.serialize());
  }

  /** Test nested namespace with storage config. */
  @Test
  public void testNestedNamespaceStorageConfig() {
    // Create a storage config
    PolarisStorageConfigurationInfo originalConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://my-bucket/warehouse/ns1/ns2")
            .roleARN("arn:aws:iam::123456789012:role/nested-namespace-role")
            .region("us-west-2")
            .build();

    // Create a nested Namespace entity
    NamespaceEntity entity =
        new NamespaceEntity.Builder(Namespace.of("ns1", "ns2"))
            .setCatalogId(1L)
            .setId(20L)
            .setParentId(10L)
            .setBaseLocation("s3://my-bucket/warehouse/ns1/ns2")
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(),
                originalConfig.serialize())
            .build();

    // Verify retrieval
    PolarisStorageConfigurationInfo retrievedConfig = entity.getStorageConfigurationInfo();
    assertThat(retrievedConfig).isNotNull();
    assertThat(retrievedConfig.serialize()).isEqualTo(originalConfig.serialize());

    // Verify namespace hierarchy
    assertThat(entity.asNamespace()).isEqualTo(Namespace.of("ns1", "ns2"));
  }

  /** Test entity with no storage config returns null. */
  @Test
  public void testNamespaceEntityWithNoConfigReturnsNull() {
    // Create an entity without storage config
    NamespaceEntity entity =
        new NamespaceEntity.Builder(Namespace.of("test_namespace"))
            .setCatalogId(1L)
            .setId(10L)
            .setParentId(1L)
            .build();

    // Verify getStorageConfigurationInfo() returns null
    assertThat(entity.getStorageConfigurationInfo()).isNull();
  }

  /** Test Azure storage config on namespace. */
  @Test
  public void testNamespaceWithAzureStorageConfig() {
    // Create Azure storage config
    PolarisStorageConfigurationInfo config =
        AzureStorageConfigurationInfo.builder()
            .addAllowedLocations("abfss://container@myaccount.dfs.core.windows.net/namespace1")
            .tenantId("test-tenant-id")
            .build();

    // Create namespace with Azure config
    NamespaceEntity entity =
        new NamespaceEntity.Builder(Namespace.of("azure_namespace"))
            .setCatalogId(1L)
            .setId(30L)
            .setParentId(1L)
            .setBaseLocation("abfss://container@myaccount.dfs.core.windows.net/namespace1")
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(), config.serialize())
            .build();

    // Verify retrieval
    PolarisStorageConfigurationInfo retrievedConfig = entity.getStorageConfigurationInfo();
    assertThat(retrievedConfig).isNotNull();
    assertThat(retrievedConfig).isInstanceOf(AzureStorageConfigurationInfo.class);
    assertThat(retrievedConfig.serialize()).isEqualTo(config.serialize());
  }

  /** Test that base location and storage config work together. */
  @Test
  public void testNamespaceWithBaseLocationAndStorageConfig() {
    String baseLocation = "s3://my-bucket/warehouse/namespace_with_config";

    PolarisStorageConfigurationInfo config =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation(baseLocation)
            .roleARN("arn:aws:iam::123456789012:role/namespace-role")
            .region("eu-central-1")
            .build();

    NamespaceEntity entity =
        new NamespaceEntity.Builder(Namespace.of("namespace_with_config"))
            .setCatalogId(1L)
            .setId(40L)
            .setParentId(1L)
            .setBaseLocation(baseLocation)
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(), config.serialize())
            .build();

    // Verify both base location and storage config are available
    assertThat(entity.getBaseLocation()).isEqualTo(baseLocation);
    assertThat(entity.getStorageConfigurationInfo()).isNotNull();
    assertThat(entity.getStorageConfigurationInfo().getAllowedLocations()).contains(baseLocation);
  }

  /** Test empty namespace (root level). */
  @Test
  public void testRootLevelNamespaceWithStorageConfig() {
    PolarisStorageConfigurationInfo config =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://my-bucket/warehouse/root")
            .roleARN("arn:aws:iam::123456789012:role/root-ns-role")
            .region("us-west-1")
            .build();

    NamespaceEntity entity =
        new NamespaceEntity.Builder(Namespace.of("root"))
            .setCatalogId(1L)
            .setId(50L)
            .setParentId(1L)
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(), config.serialize())
            .build();

    assertThat(entity.getStorageConfigurationInfo()).isNotNull();
    assertThat(entity.getParentNamespace()).isEqualTo(Namespace.empty());
  }
}

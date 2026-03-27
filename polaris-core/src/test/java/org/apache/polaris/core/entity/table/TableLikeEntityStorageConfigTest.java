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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for storage configuration support in TableLikeEntity.
 *
 * <p>Tests storage config getter method for table entities. Since TableLikeEntity is abstract, we
 * use IcebergTableLikeEntity as the concrete implementation.
 */
public class TableLikeEntityStorageConfigTest {

  /** Test serialization round-trip for IcebergTable entity with storage config. */
  @Test
  public void testIcebergTableEntityStorageConfigRoundTrip() {
    // Create a storage config
    PolarisStorageConfigurationInfo originalConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://my-bucket/warehouse/table1")
            .roleARN("arn:aws:iam::123456789012:role/example-role")
            .region("us-west-2")
            .build();

    // Create an IcebergTable entity using the Builder with internal properties
    IcebergTableLikeEntity entity =
        new IcebergTableLikeEntity.Builder(
                org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_TABLE,
                TableIdentifier.of("namespace1", "test_table"),
                "s3://my-bucket/warehouse/table1/metadata/v1.metadata.json")
            .setCatalogId(1L)
            .setId(100L)
            .setParentId(10L)
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

  /** Test GenericTable entity with storage config. */
  @Test
  public void testGenericTableEntityStorageConfigRoundTrip() {
    // Create a storage config
    PolarisStorageConfigurationInfo originalConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://my-bucket/warehouse/generic_table")
            .roleARN("arn:aws:iam::123456789012:role/generic-table-role")
            .region("eu-west-1")
            .build();

    // Create a GenericTable entity
    GenericTableEntity entity =
        new GenericTableEntity.Builder(TableIdentifier.of("namespace1", "generic_test"), "parquet")
            .setCatalogId(1L)
            .setId(200L)
            .setParentId(10L)
            .setBaseLocation("s3://my-bucket/warehouse/generic_table")
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(),
                originalConfig.serialize())
            .build();

    // Verify serialization happened
    String storedJson =
        entity
            .getInternalPropertiesAsMap()
            .get(PolarisEntityConstants.getStorageConfigInfoPropertyName());
    assertThat(storedJson).isNotNull();

    // Verify retrieval and deserialization
    PolarisStorageConfigurationInfo retrievedConfig = entity.getStorageConfigurationInfo();
    assertThat(retrievedConfig).isNotNull();
    assertThat(retrievedConfig.serialize()).isEqualTo(originalConfig.serialize());
  }

  /** Test entity with no storage config returns null. */
  @Test
  public void testTableEntityWithNoConfigReturnsNull() {
    // Create an entity without storage config
    IcebergTableLikeEntity entity =
        new IcebergTableLikeEntity.Builder(
                org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_TABLE,
                TableIdentifier.of("namespace1", "test_table"),
                "s3://my-bucket/warehouse/table1/metadata/v1.metadata.json")
            .setCatalogId(1L)
            .setId(100L)
            .setParentId(10L)
            .build();

    // Verify getStorageConfigurationInfo() returns null
    assertThat(entity.getStorageConfigurationInfo()).isNull();
  }

  /**
   * Test that StorageConfigurationInfo can be retrieved after setting through internal properties.
   */
  @Test
  public void testStorageConfigThroughInternalProperties() {
    PolarisStorageConfigurationInfo config =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://my-bucket/warehouse/builder-test")
            .roleARN("arn:aws:iam::123456789012:role/builder-test-role")
            .region("ap-south-1")
            .build();

    // Create entity with storage config via builder internal properties
    IcebergTableLikeEntity entity =
        new IcebergTableLikeEntity.Builder(
                org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_TABLE,
                TableIdentifier.of("namespace1", "builder_test_table"),
                "s3://my-bucket/warehouse/builder_test_table/metadata/v1.metadata.json")
            .setCatalogId(1L)
            .setId(100L)
            .setParentId(10L)
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(), config.serialize())
            .build();

    // Verify retrieval
    PolarisStorageConfigurationInfo retrievedConfig = entity.getStorageConfigurationInfo();
    assertThat(retrievedConfig).isNotNull();
    assertThat(retrievedConfig.serialize()).isEqualTo(config.serialize());
  }

  /** Test IcebergView entity can also have storage config. */
  @Test
  public void testIcebergViewEntityStorageConfig() {
    PolarisStorageConfigurationInfo config =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://my-bucket/warehouse/view1")
            .roleARN("arn:aws:iam::123456789012:role/view-role")
            .region("us-east-1")
            .build();

    // Create an IcebergView entity
    IcebergTableLikeEntity entity =
        new IcebergTableLikeEntity.Builder(
                org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_VIEW,
                TableIdentifier.of("namespace1", "test_view"),
                "s3://my-bucket/warehouse/view1/metadata/v1.metadata.json")
            .setCatalogId(1L)
            .setId(300L)
            .setParentId(10L)
            .addInternalProperty(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(), config.serialize())
            .build();

    // Verify retrieval
    PolarisStorageConfigurationInfo retrievedConfig = entity.getStorageConfigurationInfo();
    assertThat(retrievedConfig).isNotNull();
    assertThat(retrievedConfig.serialize()).isEqualTo(config.serialize());
  }
}

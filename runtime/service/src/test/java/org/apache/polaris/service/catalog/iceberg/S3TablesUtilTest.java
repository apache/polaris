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
package org.apache.polaris.service.catalog.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.storage.aws.AwsS3TablesStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.junit.jupiter.api.Test;

class S3TablesUtilTest {

  private static final String BUCKET_ARN =
      "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket";
  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/polaris-s3tables-role";
  private static final String S3_BASE_LOCATION = "s3://my-bucket/path";
  private static final String TABLE_ID = "abc-123";

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Builds a CatalogEntity whose storage config is S3_TABLES. The bucket ARN is used as both the
   * allowed location and the default-base-location.
   */
  private CatalogEntity s3TablesCatalog() {
    AwsS3TablesStorageConfigurationInfo s3TablesConfig =
        AwsS3TablesStorageConfigurationInfo.builder()
            .allowedLocations(Set.of(BUCKET_ARN))
            .roleARN(ROLE_ARN)
            .region("us-east-1")
            .build();

    return new CatalogEntity.Builder()
        .setName("s3tables-catalog")
        .setDefaultBaseLocation(BUCKET_ARN)
        .addInternalProperty(
            PolarisEntityConstants.getStorageConfigInfoPropertyName(), s3TablesConfig.serialize())
        .build();
  }

  /**
   * Builds a CatalogEntity whose storage config is plain S3. The S3 URI is used as both the
   * allowed location and the default-base-location.
   */
  private CatalogEntity s3Catalog() {
    AwsStorageConfigurationInfo s3Config =
        AwsStorageConfigurationInfo.builder()
            .allowedLocations(Set.of(S3_BASE_LOCATION))
            .roleARN(ROLE_ARN)
            .build();

    return new CatalogEntity.Builder()
        .setName("s3-catalog")
        .setDefaultBaseLocation(S3_BASE_LOCATION)
        .addInternalProperty(
            PolarisEntityConstants.getStorageConfigInfoPropertyName(), s3Config.serialize())
        .build();
  }

  /** Builds a CatalogEntity with no storage config (internal properties are empty). */
  private CatalogEntity noStorageConfigCatalog() {
    return new CatalogEntity.Builder().setName("no-storage-catalog").build();
  }

  // ---------------------------------------------------------------------------
  // isS3TablesCatalog
  // ---------------------------------------------------------------------------

  @Test
  void isS3TablesCatalog_returnsTrueForS3TablesConfig() {
    assertThat(S3TablesUtil.isS3TablesCatalog(s3TablesCatalog())).isTrue();
  }

  @Test
  void isS3TablesCatalog_returnsFalseForS3Config() {
    assertThat(S3TablesUtil.isS3TablesCatalog(s3Catalog())).isFalse();
  }

  @Test
  void isS3TablesCatalog_returnsFalseWhenNoStorageConfig() {
    assertThat(S3TablesUtil.isS3TablesCatalog(noStorageConfigCatalog())).isFalse();
  }

  // ---------------------------------------------------------------------------
  // constructTableArn
  // ---------------------------------------------------------------------------

  @Test
  void constructTableArn_appendsTableSegmentToBaseLocation() {
    CatalogEntity catalog = s3TablesCatalog();
    String arn = S3TablesUtil.constructTableArn(catalog, TABLE_ID);
    assertThat(arn).isEqualTo(BUCKET_ARN + "/table/" + TABLE_ID);
  }

  // ---------------------------------------------------------------------------
  // validateTableArn
  // ---------------------------------------------------------------------------

  @Test
  void validateTableArn_doesNotThrowWhenArnIsUnderAllowedLocation() {
    CatalogEntity catalog = s3TablesCatalog();
    TableIdentifier tableId = TableIdentifier.of("ns", "tbl");
    String tableArn = BUCKET_ARN + "/table/" + TABLE_ID;

    // Should not throw
    S3TablesUtil.validateTableArn(tableId, tableArn, catalog);
  }

  @Test
  void validateTableArn_throwsForbiddenExceptionWhenArnIsOutsideAllowedLocations() {
    CatalogEntity catalog = s3TablesCatalog();
    TableIdentifier tableId = TableIdentifier.of("ns", "tbl");
    // Bucket ARN that is NOT in the allowed locations
    String outsideArn = "arn:aws:s3tables:us-east-1:999999999999:bucket/other-bucket/table/xyz";

    assertThatThrownBy(() -> S3TablesUtil.validateTableArn(tableId, outsideArn, catalog))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining(outsideArn);
  }

  // ---------------------------------------------------------------------------
  // resolveTableLocations
  // ---------------------------------------------------------------------------

  @Test
  void resolveTableLocations_returnsOriginalLocationsForNonS3TablesCatalog() {
    CatalogEntity catalog = s3Catalog();
    TableIdentifier tableId = TableIdentifier.of("ns", "tbl");
    Set<String> originalLocations = Set.of(S3_BASE_LOCATION + "/ns/tbl");

    Set<String> result =
        S3TablesUtil.resolveTableLocations(
            tableId, originalLocations, catalog, Optional.of(TABLE_ID));

    assertThat(result).isEqualTo(originalLocations);
  }

  @Test
  void resolveTableLocations_returnsArnSetWhenS3TablesAndTableIdPresent() {
    CatalogEntity catalog = s3TablesCatalog();
    TableIdentifier tableId = TableIdentifier.of("ns", "tbl");
    Set<String> originalLocations = Set.of(S3_BASE_LOCATION + "/ns/tbl");
    String expectedArn = BUCKET_ARN + "/table/" + TABLE_ID;

    Set<String> result =
        S3TablesUtil.resolveTableLocations(
            tableId, originalLocations, catalog, Optional.of(TABLE_ID));

    assertThat(result).containsExactly(expectedArn);
  }

  @Test
  void resolveTableLocations_throwsBadRequestExceptionWhenS3TablesAndTableIdAbsent() {
    CatalogEntity catalog = s3TablesCatalog();
    TableIdentifier tableId = TableIdentifier.of("ns", "tbl");
    Set<String> originalLocations = Set.of(S3_BASE_LOCATION + "/ns/tbl");

    assertThatThrownBy(
            () ->
                S3TablesUtil.resolveTableLocations(
                    tableId, originalLocations, catalog, Optional.empty()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("no tableId");
  }
}

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
package org.apache.polaris.service.catalog.validation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo.StorageType;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.r2.R2StorageConfigurationInfo;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class StorageTypeFileIOTest {

  private static final String S3_FILE_IO = "org.apache.iceberg.aws.s3.S3FileIO";
  private static final String ACCOUNT = "0123456789abcdef0123456789abcdef";

  private static RealmConfig realmConfigSupporting(String... types) {
    RealmConfig realmConfig = Mockito.mock(RealmConfig.class);
    when(realmConfig.getConfig(FeatureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES))
        .thenReturn(List.of(types));
    when(realmConfig.getConfig(FeatureConfiguration.ALLOW_INSECURE_STORAGE_TYPES))
        .thenReturn(false);
    when(realmConfig.getConfig(FeatureConfiguration.ALLOW_SPECIFYING_FILE_IO_IMPL))
        .thenReturn(true);
    return realmConfig;
  }

  @Test
  void everyCoreStorageTypeHasAFileIoEntry() {
    for (StorageType type : StorageType.values()) {
      assertThatCode(() -> StorageTypeFileIO.valueOf(type.name())).doesNotThrowAnyException();
    }
  }

  @Test
  void s3FileIoResolvesToS3ByDefault() {
    assertThat(StorageTypeFileIO.fromFileIoImplementation(S3_FILE_IO))
        .isEqualTo(StorageTypeFileIO.S3);
  }

  @Test
  void s3FileIoResolvesToPreferredTypeWhenItIsACandidate() {
    assertThat(StorageTypeFileIO.fromFileIoImplementation(S3_FILE_IO, StorageType.R2))
        .isEqualTo(StorageTypeFileIO.R2);
    assertThat(StorageTypeFileIO.fromFileIoImplementation(S3_FILE_IO, StorageType.S3))
        .isEqualTo(StorageTypeFileIO.S3);
    assertThat(StorageTypeFileIO.fromFileIoImplementation(S3_FILE_IO, StorageType.GCS))
        .isEqualTo(StorageTypeFileIO.S3);
  }

  @Test
  void r2CatalogPassesWhenOnlyR2IsSupported() {
    R2StorageConfigurationInfo r2 =
        R2StorageConfigurationInfo.builder()
            .accountId(ACCOUNT)
            .addAllowedLocations("s3://bucket/")
            .build();
    String io =
        IcebergPropertiesValidation.determineFileIOClassName(
            realmConfigSupporting("R2"), Map.of(), r2);
    assertThat(io).isEqualTo(S3_FILE_IO);
  }

  @Test
  void s3CatalogPassesWhenOnlyS3IsSupported() {
    AwsStorageConfigurationInfo s3 =
        AwsStorageConfigurationInfo.builder().addAllowedLocations("s3://bucket/").build();
    String io =
        IcebergPropertiesValidation.determineFileIOClassName(
            realmConfigSupporting("S3"), Map.of(), s3);
    assertThat(io).isEqualTo(S3_FILE_IO);
  }

  @Test
  void s3CatalogFailsWhenOnlyR2IsSupported() {
    AwsStorageConfigurationInfo s3 =
        AwsStorageConfigurationInfo.builder().addAllowedLocations("s3://bucket/").build();
    assertThatThrownBy(
            () ->
                IcebergPropertiesValidation.determineFileIOClassName(
                    realmConfigSupporting("R2"), Map.of(), s3))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("not supported");
  }

  @Test
  void userSpecifiedS3FileIoOnR2CatalogResolvesToR2() {
    R2StorageConfigurationInfo r2 =
        R2StorageConfigurationInfo.builder()
            .accountId(ACCOUNT)
            .addAllowedLocations("s3://bucket/")
            .build();
    String io =
        IcebergPropertiesValidation.determineFileIOClassName(
            realmConfigSupporting("R2"), Map.of(CatalogProperties.FILE_IO_IMPL, S3_FILE_IO), r2);
    assertThat(io).isEqualTo(S3_FILE_IO);
  }

  @Test
  void r2IsASafeStorageType() {
    assertThat(IcebergPropertiesValidation.safeStorageType("R2")).isTrue();
  }

  @Test
  void r2OnlyRealmAcceptsS3FileIoWithoutAStorageConfig() {
    // validateIcebergProperties has no storage config to prefer a type with, so S3FileIO resolves
    // to S3. An R2-only realm must still accept it: it is the FileIO R2 uses.
    assertThatCode(
            () ->
                IcebergPropertiesValidation.validateIcebergProperties(
                    realmConfigSupporting("R2"),
                    Map.of(CatalogProperties.FILE_IO_IMPL, S3_FILE_IO)))
        .doesNotThrowAnyException();
    assertThat(StorageTypeFileIO.supportedInRealm(S3_FILE_IO, null, realmConfigSupporting("R2")))
        .isTrue();
  }

  @Test
  void fileOnlyRealmStillRejectsS3FileIoWithoutAStorageConfig() {
    assertThatThrownBy(
            () ->
                IcebergPropertiesValidation.validateIcebergProperties(
                    realmConfigSupporting("FILE"),
                    Map.of(CatalogProperties.FILE_IO_IMPL, S3_FILE_IO)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("not supported");
    assertThat(StorageTypeFileIO.supportedInRealm(S3_FILE_IO, null, realmConfigSupporting("FILE")))
        .isFalse();
  }

  @Test
  void aKnownStorageTypeStillNarrowsTheCheck() {
    // With the catalog's type in hand, only that type counts: an S3 catalog in an R2-only realm
    // stays rejected even though S3FileIO is acceptable to R2.
    assertThat(
            StorageTypeFileIO.supportedInRealm(
                S3_FILE_IO, StorageType.S3, realmConfigSupporting("R2")))
        .isFalse();
    assertThat(
            StorageTypeFileIO.supportedInRealm(
                S3_FILE_IO, StorageType.R2, realmConfigSupporting("R2")))
        .isTrue();
  }
}

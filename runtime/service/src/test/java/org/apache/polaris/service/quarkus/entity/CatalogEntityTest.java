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
package org.apache.polaris.service.quarkus.entity;

import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.AzureStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.GcpStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class CatalogEntityTest {

  private CallContext callContext;

  @BeforeEach
  public void setup() {
    MetaStoreManagerFactory metaStoreManagerFactory = new InMemoryPolarisMetaStoreManagerFactory();
    RealmContext realmContext = () -> "realm";
    PolarisCallContext polarisCallContext =
        new PolarisCallContext(
            realmContext,
            metaStoreManagerFactory.getOrCreateSessionSupplier(() -> "realm").get(),
            new PolarisDefaultDiagServiceImpl());
    this.callContext = polarisCallContext;
    CallContext.setCurrentContext(polarisCallContext);
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3", "s3a"})
  public void testInvalidAllowedLocationPrefixS3(String scheme) {
    String storageLocation = "unsupportPrefix://mybucket/path";
    AwsStorageConfigInfo awsStorageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(storageLocation, scheme + "://externally-owned-bucket"))
            .build();
    CatalogProperties props = new CatalogProperties(storageLocation);
    Catalog awsCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(props)
            .setStorageConfigInfo(awsStorageConfigModel)
            .build();
    Assertions.assertThatThrownBy(() -> CatalogEntity.fromCatalog(callContext, awsCatalog))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Location prefix not allowed: 'unsupportPrefix://mybucket/path', expected prefixes");
  }

  @Test
  public void testInvalidAllowedLocationPrefix() {
    String storageLocation = "unsupportPrefix://mybucket/path";

    // Invalid azure prefix
    AzureStorageConfigInfo azureStorageConfigModel =
        AzureStorageConfigInfo.builder()
            .setAllowedLocations(
                List.of(storageLocation, "abfs://container@storageaccount.blob.windows.net/path"))
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setTenantId("tenantId")
            .build();
    Catalog azureCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(
                new CatalogProperties("abfs://container@storageaccount.blob.windows.net/path"))
            .setStorageConfigInfo(azureStorageConfigModel)
            .build();
    Assertions.assertThatThrownBy(() -> CatalogEntity.fromCatalog(callContext, azureCatalog))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid azure location uri unsupportPrefix://mybucket/path");

    // invalid gcp prefix
    GcpStorageConfigInfo gcpStorageConfigModel =
        GcpStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
            .setAllowedLocations(List.of(storageLocation, "gs://externally-owned-bucket"))
            .build();
    Catalog gcpCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(new CatalogProperties("gs://externally-owned-bucket"))
            .setStorageConfigInfo(gcpStorageConfigModel)
            .build();
    Assertions.assertThatThrownBy(() -> CatalogEntity.fromCatalog(callContext, gcpCatalog))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Location prefix not allowed: 'unsupportPrefix://mybucket/path', expected prefixes");
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3", "s3a"})
  public void testExceedMaxAllowedLocations(String scheme) {
    String storageLocation = scheme + "://mybucket/path/";
    AwsStorageConfigInfo awsStorageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(
                List.of(
                    storageLocation + "1/",
                    storageLocation + "2/",
                    storageLocation + "3/",
                    storageLocation + "4/",
                    storageLocation + "5/",
                    storageLocation + "6/"))
            .build();
    CatalogProperties prop = new CatalogProperties(storageLocation);
    Catalog awsCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(prop)
            .setStorageConfigInfo(awsStorageConfigModel)
            .build();
    Assertions.assertThatCode(() -> CatalogEntity.fromCatalog(callContext, awsCatalog))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3", "s3a"})
  public void testValidAllowedLocationPrefixS3(String scheme) {
    String baseLocation = scheme + "://externally-owned-bucket";
    AwsStorageConfigInfo awsStorageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(baseLocation))
            .build();

    CatalogProperties prop = new CatalogProperties(baseLocation);
    Catalog awsCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(prop)
            .setStorageConfigInfo(awsStorageConfigModel)
            .build();
    Assertions.assertThatNoException()
        .isThrownBy(() -> CatalogEntity.fromCatalog(callContext, awsCatalog));
  }

  @Test
  public void testValidAllowedLocationPrefix() {
    String basedLocation = "abfs://container@storageaccount.blob.windows.net/path";
    AzureStorageConfigInfo azureStorageConfigModel =
        AzureStorageConfigInfo.builder()
            .setAllowedLocations(List.of(basedLocation))
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setTenantId("tenantId")
            .build();
    CatalogProperties prop = new CatalogProperties(basedLocation);
    Catalog awsCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(prop)
            .setStorageConfigInfo(azureStorageConfigModel)
            .build();
    Assertions.assertThatNoException()
        .isThrownBy(() -> CatalogEntity.fromCatalog(callContext, awsCatalog));
    prop.put(CatalogEntity.DEFAULT_BASE_LOCATION_KEY, basedLocation);

    Catalog azureCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(new CatalogProperties(basedLocation))
            .setStorageConfigInfo(azureStorageConfigModel)
            .build();
    Assertions.assertThatNoException()
        .isThrownBy(() -> CatalogEntity.fromCatalog(callContext, azureCatalog));

    basedLocation = "gs://externally-owned-bucket";
    prop.put(CatalogEntity.DEFAULT_BASE_LOCATION_KEY, basedLocation);
    GcpStorageConfigInfo gcpStorageConfigModel =
        GcpStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
            .setAllowedLocations(List.of(basedLocation))
            .build();
    Catalog gcpCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(new CatalogProperties(basedLocation))
            .setStorageConfigInfo(gcpStorageConfigModel)
            .build();
    Assertions.assertThatNoException()
        .isThrownBy(() -> CatalogEntity.fromCatalog(callContext, gcpCatalog));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "arn:aws:iam::0123456:role/jdoe", "aws-cn"})
  public void testInvalidArn(String roleArn) {
    String basedLocation = "s3://externally-owned-bucket";
    AwsStorageConfigInfo awsStorageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn(roleArn)
            .setExternalId("externalId")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(basedLocation))
            .build();

    CatalogProperties prop = new CatalogProperties(basedLocation);
    Catalog awsCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(prop)
            .setStorageConfigInfo(awsStorageConfigModel)
            .build();
    String expectedMessage = "";
    switch (roleArn) {
      case "":
        expectedMessage = "ARN cannot be null or empty";
        break;
      case "aws-cn":
        expectedMessage = "AWS China is temporarily not supported";
        break;
      default:
        expectedMessage = "Invalid role ARN format";
    }
    ;
    Assertions.assertThatThrownBy(() -> CatalogEntity.fromCatalog(callContext, awsCatalog))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
  }

  @Test
  public void testCatalogTypeDefaultsToInternal() {
    String baseLocation = "s3://test-bucket/path";
    AwsStorageConfigInfo storageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/test-role")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(baseLocation))
            .build();
    CatalogEntity catalogEntity =
        new CatalogEntity.Builder()
            .setName("test-catalog")
            .setDefaultBaseLocation(baseLocation)
            .setStorageConfigurationInfo(callContext, storageConfigModel, baseLocation)
            .build();

    Catalog catalog = catalogEntity.asCatalog();
    Assertions.assertThat(catalog.getType()).isEqualTo(Catalog.TypeEnum.INTERNAL);
  }

  @Test
  public void testCatalogTypeExternalPreserved() {
    String baseLocation = "s3://test-bucket/path";
    AwsStorageConfigInfo storageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/test-role")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(baseLocation))
            .build();
    CatalogEntity catalogEntity =
        new CatalogEntity.Builder()
            .setName("test-external-catalog")
            .setDefaultBaseLocation(baseLocation)
            .setCatalogType(Catalog.TypeEnum.EXTERNAL.name())
            .setStorageConfigurationInfo(callContext, storageConfigModel, baseLocation)
            .build();

    Catalog catalog = catalogEntity.asCatalog();
    Assertions.assertThat(catalog.getType()).isEqualTo(Catalog.TypeEnum.EXTERNAL);
  }

  @Test
  public void testCatalogTypeInternalExplicitlySet() {
    String baseLocation = "s3://test-bucket/path";
    AwsStorageConfigInfo storageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/test-role")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(baseLocation))
            .build();
    CatalogEntity catalogEntity =
        new CatalogEntity.Builder()
            .setName("test-internal-catalog")
            .setDefaultBaseLocation(baseLocation)
            .setCatalogType(Catalog.TypeEnum.INTERNAL.name())
            .setStorageConfigurationInfo(callContext, storageConfigModel, baseLocation)
            .build();

    Catalog catalog = catalogEntity.asCatalog();
    Assertions.assertThat(catalog.getType()).isEqualTo(Catalog.TypeEnum.INTERNAL);
  }
}

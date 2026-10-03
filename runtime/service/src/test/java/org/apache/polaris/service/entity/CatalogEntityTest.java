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
package org.apache.polaris.service.entity;

import static org.apache.polaris.core.config.RealmConfigurationSource.EMPTY_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.AwsIamServiceIdentityInfo;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.AzureStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.GcpStorageConfigInfo;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.ServiceIdentityInfo;
import org.apache.polaris.core.admin.model.SigV4AuthenticationParameters;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.config.RealmConfigurationSource;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.identity.credential.AwsIamServiceIdentityCredential;
import org.apache.polaris.core.identity.dpo.AwsIamServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

public class CatalogEntityTest {
  private static final ObjectMapper MAPPER = JsonMapper.shared();

  private RealmConfig realmConfig;
  private ServiceIdentityProvider serviceIdentityProvider;

  @BeforeEach
  public void setup() {
    RealmContext realmContext = () -> "realm";
    this.realmConfig = new RealmConfigImpl(EMPTY_CONFIG, realmContext);
    this.serviceIdentityProvider = Mockito.mock(ServiceIdentityProvider.class);
    Mockito.when(serviceIdentityProvider.getServiceIdentityInfo(Mockito.any()))
        .thenReturn(
            Optional.of(
                AwsIamServiceIdentityInfo.builder()
                    .setIdentityType(ServiceIdentityInfo.IdentityTypeEnum.AWS_IAM)
                    .setIamArn("arn:aws:iam::123456789012:user/test-user")
                    .build()));
    Mockito.when(serviceIdentityProvider.getServiceIdentityCredential(Mockito.any()))
        .thenReturn(
            Optional.of(
                new AwsIamServiceIdentityCredential("arn:aws:iam::123456789012:user/test-user")));
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
    Assertions.assertThatThrownBy(() -> CatalogEntity.fromCatalog(realmConfig, awsCatalog))
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
    Assertions.assertThatThrownBy(() -> CatalogEntity.fromCatalog(realmConfig, azureCatalog))
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
    Assertions.assertThatThrownBy(() -> CatalogEntity.fromCatalog(realmConfig, gcpCatalog))
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
    CatalogProperties prop = new CatalogProperties(storageLocation + "1/");
    Catalog awsCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(prop)
            .setStorageConfigInfo(awsStorageConfigModel)
            .build();
    Assertions.assertThatCode(() -> CatalogEntity.fromCatalog(realmConfig, awsCatalog))
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
        .isThrownBy(() -> CatalogEntity.fromCatalog(realmConfig, awsCatalog));
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
        .isThrownBy(() -> CatalogEntity.fromCatalog(realmConfig, awsCatalog));
    prop.put(CatalogEntity.DEFAULT_BASE_LOCATION_KEY, basedLocation);

    Catalog azureCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(new CatalogProperties(basedLocation))
            .setStorageConfigInfo(azureStorageConfigModel)
            .build();
    Assertions.assertThatNoException()
        .isThrownBy(() -> CatalogEntity.fromCatalog(realmConfig, azureCatalog));

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
        .isThrownBy(() -> CatalogEntity.fromCatalog(realmConfig, gcpCatalog));
  }

  @Test
  public void testEmptyAllowedLocationsAutoPopulatedAtCreate() {
    // Convenience for the simple-create case: if the caller supplies a default-base-location
    // but no allowed-locations, the Builder defaults allowed-locations to [defaultBaseLocation].
    String basedLocation = "s3://my-bucket/data/";
    AwsStorageConfigInfo awsStorageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            // No setAllowedLocations() - user supplied only the base.
            .build();
    Catalog awsCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(new CatalogProperties(basedLocation))
            .setStorageConfigInfo(awsStorageConfigModel)
            .build();
    CatalogEntity entity = CatalogEntity.fromCatalog(realmConfig, awsCatalog);
    Assertions.assertThat(entity.getStorageConfigurationInfo().getAllowedLocations())
        .containsExactly(basedLocation);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedKmsKeysMigratedToEncryptionKeys() {
    String baseLocation = "s3://my-bucket/data/";
    String currentKmsKey =
        "arn:aws:kms:us-east-1:012345678901:key/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    String allowedKmsKey =
        "arn:aws:kms:us-east-1:012345678901:key/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
    AwsStorageConfigInfo awsStorageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setCurrentKmsKey(currentKmsKey)
            .setAllowedKmsKeys(List.of(allowedKmsKey))
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .build();
    Catalog awsCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(new CatalogProperties(baseLocation))
            .setStorageConfigInfo(awsStorageConfigModel)
            .build();

    CatalogEntity entity = CatalogEntity.fromCatalog(realmConfig, awsCatalog);
    AwsStorageConfigInfo response =
        (AwsStorageConfigInfo) entity.asCatalog().getStorageConfigInfo();

    assertThat(response.getCurrentKmsKey()).isNull();
    assertThat(response.getAllowedKmsKeys()).containsExactly(allowedKmsKey, currentKmsKey);
    assertThat(response.getEncryptionKeys()).containsExactly(allowedKmsKey, currentKmsKey);
  }

  @Test
  public void testExplicitAllowedLocationsWithBaseInside_storedAsIs() {
    // User supplied explicit allowed-locations containing the default-base-location: store the
    // list as-is. No silent additions to the user-supplied perimeter.
    String basedLocation = "s3://my-bucket/data/";
    AwsStorageConfigInfo awsStorageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://my-bucket/"))
            .build();
    Catalog awsCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(new CatalogProperties(basedLocation))
            .setStorageConfigInfo(awsStorageConfigModel)
            .build();
    CatalogEntity entity = CatalogEntity.fromCatalog(realmConfig, awsCatalog);
    Assertions.assertThat(entity.getStorageConfigurationInfo().getAllowedLocations())
        .containsExactly("s3://my-bucket/");
  }

  @Test
  public void testExplicitAllowedLocationsWithBaseOutside_rejected() {
    // User-supplied explicit allowed-locations not containing the default-base-location: reject
    // with a clear error rather than silently widening the allowed perimeter to include the base.
    AwsStorageConfigInfo awsStorageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://other-bucket/"))
            .build();
    Catalog awsCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("name")
            .setProperties(new CatalogProperties("s3://my-bucket/data/"))
            .setStorageConfigInfo(awsStorageConfigModel)
            .build();
    Assertions.assertThatThrownBy(() -> CatalogEntity.fromCatalog(realmConfig, awsCatalog))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("s3://my-bucket/data/")
        .hasMessageContaining("s3://other-bucket/");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "arn:aws:iam:0123456:role/jdoe", "arn:aws-cn:iam:0123456:role/jdoe"})
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
    String expectedMessage =
        roleArn.isEmpty() ? "ARN must not be empty" : "Invalid role ARN format: " + roleArn;
    Assertions.assertThatThrownBy(() -> CatalogEntity.fromCatalog(realmConfig, awsCatalog))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "arn:aws:iam::012345678911:role/rollerblade",
        "test:test:iam:region:accountid:role/rollerblade",
        "a::iam:::role/rollerblade"
      })
  public void testValidArn(String roleArn) {
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
    Assertions.assertThatNoException()
        .isThrownBy(() -> CatalogEntity.fromCatalog(realmConfig, awsCatalog));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCatalogTypeDefaultsToInternal() {
    String baseLocation = "s3://test-bucket/path";
    AwsStorageConfigInfo storageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/test-role")
            .setExternalId("externalId")
            .setEncryptionKeys(List.of("arn:aws:kms:us-east-1:012345678901:key/444343245"))
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(baseLocation))
            .build();
    CatalogEntity catalogEntity =
        new CatalogEntity.Builder()
            .setName("test-catalog")
            .setDefaultBaseLocation(baseLocation)
            .setStorageConfigurationInfo(realmConfig, storageConfigModel)
            .build();

    Catalog catalog = catalogEntity.asCatalog(serviceIdentityProvider);
    assertThat(catalog.getType()).isEqualTo(Catalog.TypeEnum.INTERNAL);
    AwsStorageConfigInfo response = (AwsStorageConfigInfo) catalog.getStorageConfigInfo();
    assertThat(response.getCurrentKmsKey()).isNull();
    assertThat(response.getAllowedKmsKeys())
        .containsExactly("arn:aws:kms:us-east-1:012345678901:key/444343245");
    assertThat(response.getEncryptionKeys())
        .containsExactly("arn:aws:kms:us-east-1:012345678901:key/444343245");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCatalogTypeExternalPreserved() {
    String baseLocation = "s3://test-bucket/path";
    AwsStorageConfigInfo storageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/test-role")
            .setEncryptionKeys(List.of("arn:aws:kms:us-east-1:012345678901:key/444343245"))
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
            .setStorageConfigurationInfo(realmConfig, storageConfigModel)
            .build();

    Catalog catalog = catalogEntity.asCatalog(serviceIdentityProvider);
    assertThat(catalog.getType()).isEqualTo(Catalog.TypeEnum.EXTERNAL);
    AwsStorageConfigInfo response = (AwsStorageConfigInfo) catalog.getStorageConfigInfo();
    assertThat(response.getCurrentKmsKey()).isNull();
    assertThat(response.getAllowedKmsKeys())
        .containsExactly("arn:aws:kms:us-east-1:012345678901:key/444343245");
    assertThat(response.getEncryptionKeys())
        .containsExactly("arn:aws:kms:us-east-1:012345678901:key/444343245");
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
            .setStorageConfigurationInfo(realmConfig, storageConfigModel)
            .build();

    Catalog catalog = catalogEntity.asCatalog(serviceIdentityProvider);
    assertThat(catalog.getType()).isEqualTo(Catalog.TypeEnum.INTERNAL);
  }

  @Test
  public void testAwsConfigJsonPropertiesPresence() {
    AwsStorageConfigInfo.Builder b =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setRoleArn("arn:aws:iam::012345678901:role/test-role");
    assertThat(MAPPER.writeValueAsString(b.build())).contains("roleArn");
    assertThat(MAPPER.writeValueAsString(b.build())).doesNotContain("endpoint");
    assertThat(MAPPER.writeValueAsString(b.build())).doesNotContain("stsEndpoint");

    b.setEndpoint("http://s3.example.com");
    b.setStsEndpoint("http://sts.example.com");
    b.setPathStyleAccess(false);
    assertThat(MAPPER.writeValueAsString(b.build())).contains("roleArn");
    assertThat(MAPPER.writeValueAsString(b.build())).contains("endpoint");
    assertThat(MAPPER.writeValueAsString(b.build())).contains("stsEndpoint");
    assertThat(MAPPER.writeValueAsString(b.build())).contains("pathStyleAccess");
  }

  @ParameterizedTest
  @MethodSource
  public void testStorageConfigRoundTrip(StorageConfigInfo config) {
    String configStr = MAPPER.writeValueAsString(config);
    CatalogEntity catalogEntity =
        new CatalogEntity.Builder()
            .setName("testStorageConfigRoundTrip")
            .setDefaultBaseLocation(config.getAllowedLocations().getFirst())
            .setCatalogType(Catalog.TypeEnum.INTERNAL.name())
            .setStorageConfigurationInfo(
                realmConfig, MAPPER.readValue(configStr, StorageConfigInfo.class))
            .build();

    Catalog catalog = catalogEntity.asCatalog(serviceIdentityProvider);
    assertThat(catalog.getStorageConfigInfo()).isEqualTo(config);
    assertThat(MAPPER.writeValueAsString(catalog.getStorageConfigInfo())).isEqualTo(configStr);
  }

  @Test
  public void testServiceIdentityInjection() {
    String baseLocation = "s3://test-bucket/path";
    AwsStorageConfigInfo storageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/test-role")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(baseLocation))
            .build();
    IcebergRestConnectionConfigInfo icebergRestConnectionConfigInfoModel =
        IcebergRestConnectionConfigInfo.builder()
            .setConnectionType(ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
            .setUri("https://glue.us-west-2.amazonaws.com")
            .setAuthenticationParameters(
                SigV4AuthenticationParameters.builder()
                    .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.SIGV4)
                    .setRoleArn("arn:aws:iam::123456789012:role/test-role")
                    .setSigningName("glue")
                    .setSigningRegion("us-west-2")
                    .build())
            .build();
    CatalogEntity catalogEntity =
        new CatalogEntity.Builder()
            .setName("test-catalog")
            .setCatalogType(Catalog.TypeEnum.EXTERNAL.name())
            .setDefaultBaseLocation(baseLocation)
            .setStorageConfigurationInfo(realmConfig, storageConfigModel)
            .setConnectionConfigInfoDpoWithSecrets(
                icebergRestConnectionConfigInfoModel, null, new AwsIamServiceIdentityInfoDpo(null))
            .build();

    Catalog catalog = catalogEntity.asCatalog(serviceIdentityProvider);
    assertThat(catalog.getType()).isEqualTo(Catalog.TypeEnum.EXTERNAL);
    ExternalCatalog externalCatalog = (ExternalCatalog) catalog;
    assertThat(externalCatalog.getConnectionConfigInfo().getConnectionType())
        .isEqualTo(ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST);
    assertThat(externalCatalog.getConnectionConfigInfo().getUri())
        .isEqualTo("https://glue.us-west-2.amazonaws.com");

    AuthenticationParameters authParams =
        externalCatalog.getConnectionConfigInfo().getAuthenticationParameters();
    assertThat(authParams.getAuthenticationType())
        .isEqualTo(AuthenticationParameters.AuthenticationTypeEnum.SIGV4);
    SigV4AuthenticationParameters sigV4AuthParams = (SigV4AuthenticationParameters) authParams;
    assertThat(sigV4AuthParams.getSigningName()).isEqualTo("glue");
    assertThat(sigV4AuthParams.getSigningRegion()).isEqualTo("us-west-2");
    assertThat(sigV4AuthParams.getRoleArn()).isEqualTo("arn:aws:iam::123456789012:role/test-role");

    ServiceIdentityInfo serviceIdentity =
        externalCatalog.getConnectionConfigInfo().getServiceIdentity();
    assertThat(serviceIdentity.getIdentityType())
        .isEqualTo(ServiceIdentityInfo.IdentityTypeEnum.AWS_IAM);
    AwsIamServiceIdentityInfo awsIamServiceIdentity = (AwsIamServiceIdentityInfo) serviceIdentity;
    assertThat(awsIamServiceIdentity.getIamArn())
        .isEqualTo("arn:aws:iam::123456789012:user/test-user");
  }

  public static Stream<Arguments> testStorageConfigRoundTrip() {
    AwsStorageConfigInfo.Builder b =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://example.com"))
            .setRoleArn("arn:aws:iam::012345678901:role/test-role");
    AzureStorageConfigInfo.Builder a =
        AzureStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setTenantId("test-tenant")
            .setAllowedLocations(List.of("abfss://test@example.dfs.core.windows.net/"));
    return Stream.of(
        Arguments.of(b.build()),
        Arguments.of(b.setExternalId("ex1").build()),
        Arguments.of(b.setRegion("us-west-2").build()),
        Arguments.of(b.setEndpoint("http://s3.example.com:1234").build()),
        Arguments.of(b.setStsEndpoint("http://sts.example.com:1234").build()),
        Arguments.of(b.setPathStyleAccess(true).build()),
        Arguments.of(b.setStorageName("my-storage").build()),
        Arguments.of(a.build()),
        Arguments.of(a.setHierarchical(true).build()));
  }

  @Test
  public void testAzureConfigJsonPropertiesPresence() {
    AzureStorageConfigInfo.Builder b =
        AzureStorageConfigInfo.builder().setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE);
    assertThat(MAPPER.writeValueAsString(b.build())).contains("storageType");
    assertThat(MAPPER.writeValueAsString(b.build())).doesNotContain("hierarchical");

    b.setHierarchical(true);
    assertThat(MAPPER.writeValueAsString(b.build())).contains("hierarchical");
  }

  @ParameterizedTest(name = "[{index}] base={1} within allowed={0}")
  @CsvSource({"s3://bucket/, s3://bucket/path/to/data", "s3://bucket/, s3://bucket/"})
  public void testBaseWithinAllowed_accepted(String allowed, String base) {
    assertThatCode(
            () ->
                new CatalogEntity.Builder()
                    .setName("test-catalog")
                    .setDefaultBaseLocation(base)
                    .setStorageConfigurationInfo(
                        realmConfig,
                        AwsStorageConfigInfo.builder()
                            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
                            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                            .setAllowedLocations(List.of(allowed))
                            .build())
                    .build())
        .doesNotThrowAnyException();
  }

  @Test
  public void testAcceptedUnderAnyOfMultipleAllowedLocations() {
    assertThatCode(
            () ->
                new CatalogEntity.Builder()
                    .setName("test-catalog")
                    .setDefaultBaseLocation("s3://bucket-b/warehouse/data")
                    .setStorageConfigurationInfo(
                        realmConfig,
                        AwsStorageConfigInfo.builder()
                            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
                            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                            .setAllowedLocations(
                                List.of("s3://bucket-a/", "s3://bucket-b/warehouse/"))
                            .build())
                    .build())
        .doesNotThrowAnyException();
  }

  @Test
  public void testBaseOutsideAllowed_rejected() {
    assertThatThrownBy(
            () ->
                new CatalogEntity.Builder()
                    .setName("test-catalog")
                    .setDefaultBaseLocation("s3://other-bucket/data")
                    .setStorageConfigurationInfo(
                        realmConfig,
                        AwsStorageConfigInfo.builder()
                            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
                            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                            .setAllowedLocations(List.of("s3://bucket/"))
                            .build())
                    .build())
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("s3://other-bucket/data")
        .hasMessageContaining("s3://bucket/");
  }

  @Test
  public void testDifferentSchemeRejected() {
    assertThatThrownBy(
            () ->
                new CatalogEntity.Builder()
                    .setName("test-catalog")
                    .setDefaultBaseLocation("gs://bucket/data")
                    .setStorageConfigurationInfo(
                        realmConfig,
                        AwsStorageConfigInfo.builder()
                            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
                            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                            .setAllowedLocations(List.of("s3://bucket/"))
                            .build())
                    .build())
        .isInstanceOf(BadRequestException.class);
  }

  // --- Named storage configurations (storageConfigInfos) ---

  private static AwsStorageConfigInfo namedAwsConfig(String storageName, String... locations) {
    return AwsStorageConfigInfo.builder()
        .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
        .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
        .setAllowedLocations(List.of(locations))
        .setStorageName(storageName)
        .build();
  }

  private static AwsStorageConfigInfo defaultAwsConfig(String... locations) {
    return AwsStorageConfigInfo.builder()
        .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
        .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
        .setAllowedLocations(List.of(locations))
        .build();
  }

  @Test
  public void testNamedStorageConfigsAbsentWhenNotSet() {
    CatalogEntity entity =
        new CatalogEntity.Builder()
            .setName("test-catalog")
            .setDefaultBaseLocation("s3://bucket/")
            .setStorageConfigurationInfo(realmConfig, defaultAwsConfig("s3://bucket/"))
            .build();
    assertThat(entity.getNamedStorageConfigurationInfos()).isEmpty();
    assertThat(entity.getInternalPropertiesAsMap())
        .doesNotContainKey(PolarisEntityConstants.getStorageConfigInfosPropertyName());
  }

  @Test
  public void testNamedStorageConfigsPersistedAndRetrievable() {
    CatalogEntity entity =
        new CatalogEntity.Builder()
            .setName("test-catalog")
            .setDefaultBaseLocation("s3://bucket/")
            .setStorageConfigurationInfo(realmConfig, defaultAwsConfig("s3://bucket/"))
            .setStorageConfigurationInfos(
                realmConfig,
                List.of(
                    namedAwsConfig("hot-us-east", "s3://hot/bucket/"),
                    namedAwsConfig("cold-archive", "s3://cold/bucket/")))
            .build();

    assertThat(entity.getNamedStorageConfigurationInfos())
        .containsOnlyKeys("hot-us-east", "cold-archive");
    assertThat(entity.getNamedStorageConfigurationInfos().get("hot-us-east").getAllowedLocations())
        .containsExactly("s3://hot/bucket/");
  }

  @Test
  public void testNamedStorageConfigNameTrimmed() {
    CatalogEntity entity =
        new CatalogEntity.Builder()
            .setName("test-catalog")
            .setDefaultBaseLocation("s3://bucket/")
            .setStorageConfigurationInfo(realmConfig, defaultAwsConfig("s3://bucket/"))
            .setStorageConfigurationInfos(
                realmConfig, List.of(namedAwsConfig("  hot  ", "s3://hot/bucket/")))
            .build();

    assertThat(entity.getNamedStorageConfigurationInfos()).containsOnlyKeys("hot");
    // The entry is keyed by its trimmed name, so its own payload must carry that same name:
    // otherwise the API response and the later credential-set lookup would both see "  hot  ".
    assertThat(entity.getNamedStorageConfigurationInfos().get("hot").getStorageName())
        .isEqualTo("hot");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "   "})
  public void testNamedStorageConfigBlankNameRejected(String blankName) {
    assertThatThrownBy(
            () ->
                new CatalogEntity.Builder()
                    .setName("test-catalog")
                    .setDefaultBaseLocation("s3://bucket/")
                    .setStorageConfigurationInfos(
                        realmConfig, List.of(namedAwsConfig(blankName, "s3://hot/bucket/")))
                    .build())
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  public void testNamedStorageConfigAbsentNameRejected() {
    AwsStorageConfigInfo noName =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setAllowedLocations(List.of("s3://hot/bucket/"))
            .build();
    assertThatThrownBy(
            () ->
                new CatalogEntity.Builder()
                    .setName("test-catalog")
                    .setDefaultBaseLocation("s3://bucket/")
                    .setStorageConfigurationInfos(realmConfig, List.of(noName))
                    .build())
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  public void testNamedStorageConfigInvalidNameSyntaxRejected() {
    assertThatThrownBy(
            () ->
                new CatalogEntity.Builder()
                    .setName("test-catalog")
                    .setDefaultBaseLocation("s3://bucket/")
                    .setStorageConfigurationInfos(
                        realmConfig, List.of(namedAwsConfig("bad name!", "s3://hot/bucket/")))
                    .build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testNamedStorageConfigNameLengthBoundary() {
    String name128 = "a".repeat(128);
    assertThatCode(
            () ->
                new CatalogEntity.Builder()
                    .setName("test-catalog")
                    .setDefaultBaseLocation("s3://bucket/")
                    .setStorageConfigurationInfos(
                        realmConfig, List.of(namedAwsConfig(name128, "s3://hot/bucket/")))
                    .build())
        .doesNotThrowAnyException();

    String name129 = "a".repeat(129);
    assertThatThrownBy(
            () ->
                new CatalogEntity.Builder()
                    .setName("test-catalog")
                    .setDefaultBaseLocation("s3://bucket/")
                    .setStorageConfigurationInfos(
                        realmConfig, List.of(namedAwsConfig(name129, "s3://hot/bucket/")))
                    .build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testNamedStorageConfigDuplicateNameRejected() {
    assertThatThrownBy(
            () ->
                new CatalogEntity.Builder()
                    .setName("test-catalog")
                    .setDefaultBaseLocation("s3://bucket/")
                    .setStorageConfigurationInfos(
                        realmConfig,
                        List.of(
                            namedAwsConfig("hot", "s3://hot/bucket-1/"),
                            namedAwsConfig("hot", "s3://hot/bucket-2/")))
                    .build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testNamedStorageConfigCollidesWithDefaultNameRejected() {
    AwsStorageConfigInfo defaultConfig =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setAllowedLocations(List.of("s3://bucket/"))
            .setStorageName("hot")
            .build();
    assertThatThrownBy(
            () ->
                new CatalogEntity.Builder()
                    .setName("test-catalog")
                    .setDefaultBaseLocation("s3://bucket/")
                    .setStorageConfigurationInfo(realmConfig, defaultConfig)
                    .setStorageConfigurationInfos(
                        realmConfig, List.of(namedAwsConfig("hot", "s3://hot/bucket/")))
                    .build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testDefaultStorageConfigRenamedOntoExistingNamedConfigRejected() {
    // An update may supply only the default config while the named set is carried forward, so the
    // collision check must hold in this direction too, not only when the named array is supplied.
    CatalogEntity original =
        new CatalogEntity.Builder()
            .setName("test-catalog")
            .setDefaultBaseLocation("s3://bucket/")
            .setStorageConfigurationInfo(realmConfig, defaultAwsConfig("s3://bucket/"))
            .setStorageConfigurationInfos(
                realmConfig, List.of(namedAwsConfig("hot", "s3://hot/bucket/")))
            .build();

    AwsStorageConfigInfo renamedDefault =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setAllowedLocations(List.of("s3://bucket/"))
            .setStorageName("hot")
            .build();

    assertThatThrownBy(
            () ->
                new CatalogEntity.Builder(original)
                    .setStorageConfigurationInfo(realmConfig, renamedDefault)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("collides with the catalog's default storage configuration name");
  }

  @Test
  public void testNamedStorageConfigCaseSensitiveNamesCoexist() {
    CatalogEntity entity =
        new CatalogEntity.Builder()
            .setName("test-catalog")
            .setDefaultBaseLocation("s3://bucket/")
            .setStorageConfigurationInfos(
                realmConfig,
                List.of(
                    namedAwsConfig("hot", "s3://hot/bucket-1/"),
                    namedAwsConfig("HOT", "s3://hot/bucket-2/")))
            .build();
    assertThat(entity.getNamedStorageConfigurationInfos()).containsOnlyKeys("hot", "HOT");
  }

  @Test
  public void testNamedStorageConfigExceedsMaxLocationsRejectedIndependently() {
    RealmConfigurationSource source =
        (rc, name) -> "STORAGE_CONFIGURATION_MAX_LOCATIONS".equals(name) ? 1 : null;
    RealmConfig maxOneLocationConfig = new RealmConfigImpl(source, () -> "realm");

    assertThatThrownBy(
            () ->
                new CatalogEntity.Builder()
                    .setName("test-catalog")
                    .setDefaultBaseLocation("s3://bucket/")
                    .setStorageConfigurationInfo(
                        maxOneLocationConfig, defaultAwsConfig("s3://bucket/"))
                    .setStorageConfigurationInfos(
                        maxOneLocationConfig,
                        List.of(
                            namedAwsConfig("within-cap", "s3://within/bucket/"),
                            namedAwsConfig(
                                "over-cap", "s3://over/bucket-1/", "s3://over/bucket-2/")))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exceeds the limit");
  }

  @Test
  public void testNamedStorageConfigWrongPrefixForOwnTypeRejected() {
    AzureStorageConfigInfo azureNamedConfig =
        AzureStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setTenantId("tenant-id")
            .setAllowedLocations(List.of("s3://wrong/scheme/"))
            .setStorageName("archive")
            .build();
    assertThatThrownBy(
            () ->
                new CatalogEntity.Builder()
                    .setName("test-catalog")
                    .setDefaultBaseLocation("s3://bucket/")
                    .setStorageConfigurationInfos(realmConfig, List.of(azureNamedConfig))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid azure location uri");
  }

  @ParameterizedTest
  @MethodSource
  public void testNamedStorageConfigEmptyAllowedLocationsRejected(List<String> allowedLocations) {
    AwsStorageConfigInfo noLocations =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setAllowedLocations(allowedLocations)
            .setStorageName("hot")
            .build();
    assertThatThrownBy(
            () ->
                new CatalogEntity.Builder()
                    .setName("test-catalog")
                    .setDefaultBaseLocation("s3://bucket/")
                    .setStorageConfigurationInfos(realmConfig, List.of(noLocations))
                    .build())
        .isInstanceOf(BadRequestException.class);
  }

  public static Stream<Arguments> testNamedStorageConfigEmptyAllowedLocationsRejected() {
    return Stream.of(Arguments.of(List.of()), Arguments.of((Object) null));
  }

  @Test
  public void testNamedStorageConfigDoesNotRequireDefaultBaseLocation() {
    // Named entries have their own allowedLocations and no per-entry base-location fallback, so
    // a catalog with only named configs (no default) must not be forced to set a default base
    // location just because the default-config code path requires one.
    assertThatCode(
            () ->
                new CatalogEntity.Builder()
                    .setName("test-catalog")
                    .setStorageConfigurationInfos(
                        realmConfig, List.of(namedAwsConfig("hot", "s3://hot/bucket/")))
                    .build())
        .doesNotThrowAnyException();
  }

  @Test
  public void testNamedStorageConfigsOmittedLeavesExistingSetUntouched() {
    CatalogEntity original =
        new CatalogEntity.Builder()
            .setName("test-catalog")
            .setDefaultBaseLocation("s3://bucket/")
            .setStorageConfigurationInfo(realmConfig, defaultAwsConfig("s3://bucket/"))
            .setStorageConfigurationInfos(
                realmConfig, List.of(namedAwsConfig("hot", "s3://hot/bucket/")))
            .build();

    // Simulate an update that never calls setStorageConfigurationInfos(...): the field stays
    // null, so processStorageConfigurationInfos() must leave internalProperties untouched.
    CatalogEntity updated = new CatalogEntity.Builder(original).build();

    assertThat(updated.getNamedStorageConfigurationInfos()).containsOnlyKeys("hot");
  }

  @Test
  public void testNamedStorageConfigsEmptyArrayRemovesKeyEntirely() {
    CatalogEntity original =
        new CatalogEntity.Builder()
            .setName("test-catalog")
            .setDefaultBaseLocation("s3://bucket/")
            .setStorageConfigurationInfo(realmConfig, defaultAwsConfig("s3://bucket/"))
            .setStorageConfigurationInfos(
                realmConfig, List.of(namedAwsConfig("hot", "s3://hot/bucket/")))
            .build();

    CatalogEntity updated =
        new CatalogEntity.Builder(original)
            .setStorageConfigurationInfos(realmConfig, List.of())
            .build();

    assertThat(updated.getNamedStorageConfigurationInfos()).isEmpty();
    assertThat(updated.getInternalPropertiesAsMap())
        .doesNotContainKey(PolarisEntityConstants.getStorageConfigInfosPropertyName());
  }

  @Test
  public void testNamedStorageConfigHeterogeneousTypesPersistIndependently() {
    AzureStorageConfigInfo archiveConfig =
        AzureStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.AZURE)
            .setTenantId("tenant-id")
            .setAllowedLocations(List.of("abfs://archive@storageaccount.blob.windows.net/"))
            .setStorageName("archive")
            .build();
    CatalogEntity entity =
        new CatalogEntity.Builder()
            .setName("test-catalog")
            .setDefaultBaseLocation("s3://bucket/")
            .setStorageConfigurationInfo(realmConfig, defaultAwsConfig("s3://bucket/"))
            .setStorageConfigurationInfos(realmConfig, List.of(archiveConfig))
            .build();

    Map<String, PolarisStorageConfigurationInfo> namedConfigs =
        entity.getNamedStorageConfigurationInfos();
    assertThat(namedConfigs.get("archive").getStorageType())
        .isEqualTo(PolarisStorageConfigurationInfo.StorageType.AZURE);
    assertThat(entity.getStorageConfigurationInfo().getStorageType())
        .isEqualTo(PolarisStorageConfigurationInfo.StorageType.S3);
  }

  @Test
  public void testPreExistingCatalogWithoutNamedConfigKeyLoadsCleanly() {
    // Simulates a catalog entity persisted before this capability existed: only the legacy
    // storage_configuration_info key is present, storage_configuration_infos was never written.
    CatalogEntity original =
        new CatalogEntity.Builder()
            .setName("legacy-catalog")
            .setDefaultBaseLocation("s3://bucket/")
            .setStorageConfigurationInfo(realmConfig, defaultAwsConfig("s3://bucket/"))
            .build();

    // Simulate a fresh load from the metastore, going through the same PolarisBaseEntity ->
    // CatalogEntity path a read API call uses.
    CatalogEntity loaded = CatalogEntity.of(original);

    assertThat(loaded.getNamedStorageConfigurationInfos()).isEmpty();
    assertThatCode(loaded::asCatalog).doesNotThrowAnyException();
    Catalog asCatalog = loaded.asCatalog();
    assertThat(asCatalog.getStorageConfigInfos()).isNull();
    assertThat(asCatalog.getStorageConfigInfo()).isNotNull();
  }

  @Test
  public void testNamedStorageConfigNameNotValidatedAgainstServerCredentials() {
    // D8: acceptance of a named config depends only on the request payload and the catalog's own
    // state, never on whether the server has a matching deployment-time credential set configured
    // for that name. This change has no credential registry to check against, so any
    // syntactically valid name must be accepted.
    CatalogEntity entity =
        new CatalogEntity.Builder()
            .setName("test-catalog")
            .setDefaultBaseLocation("s3://bucket/")
            .setStorageConfigurationInfo(realmConfig, defaultAwsConfig("s3://bucket/"))
            .setStorageConfigurationInfos(
                realmConfig, List.of(namedAwsConfig("no-such-credential-set", "s3://hot/bucket/")))
            .build();

    assertThat(entity.getNamedStorageConfigurationInfos())
        .containsOnlyKeys("no-such-credential-set");
  }
}

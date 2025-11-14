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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
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
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.identity.credential.AwsIamServiceIdentityCredential;
import org.apache.polaris.core.identity.dpo.AwsIamServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class CatalogEntityTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private RealmConfig realmConfig;
  private ServiceIdentityProvider serviceIdentityProvider;

  @BeforeEach
  public void setup() {
    RealmContext realmContext = () -> "realm";
    this.realmConfig = new RealmConfigImpl(new PolarisConfigurationStore() {}, realmContext);
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
    CatalogProperties prop = new CatalogProperties(storageLocation);
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

  @ParameterizedTest
  @ValueSource(strings = {"", "arn:aws:iam:0123456:role/jdoe", "aws-cn"})
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
        switch (roleArn) {
          case "" -> "ARN must not be empty";
          case "aws-cn" -> "AWS China is temporarily not supported";
          default -> "Invalid role ARN format: arn:aws:iam:0123456:role/jdoe";
        };
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
  public void testCatalogTypeDefaultsToInternal() {
    String baseLocation = "s3://test-bucket/path";
    AwsStorageConfigInfo storageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/test-role")
            .setExternalId("externalId")
            .setKmsKeyArn("arn:aws:kms:us-east-1:012345678901:key/444343245")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(baseLocation))
            .build();
    CatalogEntity catalogEntity =
        new CatalogEntity.Builder()
            .setName("test-catalog")
            .setDefaultBaseLocation(baseLocation)
            .setStorageConfigurationInfo(realmConfig, storageConfigModel, baseLocation)
            .build();

    Catalog catalog = catalogEntity.asCatalog(serviceIdentityProvider);
    assertThat(catalog.getType()).isEqualTo(Catalog.TypeEnum.INTERNAL);
    assertThat(((AwsStorageConfigInfo) catalog.getStorageConfigInfo()).getKmsKeyArn())
        .isEqualTo("arn:aws:kms:us-east-1:012345678901:key/444343245");
  }

  @Test
  public void testCatalogTypeExternalPreserved() {
    String baseLocation = "s3://test-bucket/path";
    AwsStorageConfigInfo storageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/test-role")
            .setKmsKeyArn("arn:aws:kms:us-east-1:012345678901:key/444343245")
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
            .setStorageConfigurationInfo(realmConfig, storageConfigModel, baseLocation)
            .build();

    Catalog catalog = catalogEntity.asCatalog(serviceIdentityProvider);
    assertThat(catalog.getType()).isEqualTo(Catalog.TypeEnum.EXTERNAL);
    assertThat(((AwsStorageConfigInfo) catalog.getStorageConfigInfo()).getKmsKeyArn())
        .isEqualTo("arn:aws:kms:us-east-1:012345678901:key/444343245");
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
            .setStorageConfigurationInfo(realmConfig, storageConfigModel, baseLocation)
            .build();

    Catalog catalog = catalogEntity.asCatalog(serviceIdentityProvider);
    assertThat(catalog.getType()).isEqualTo(Catalog.TypeEnum.INTERNAL);
  }

  @Test
  public void testAwsConfigJsonPropertiesPresence() throws JsonProcessingException {
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
  public void testAwsConfigRoundTrip(AwsStorageConfigInfo config) throws JsonProcessingException {
    String configStr = MAPPER.writeValueAsString(config);
    CatalogEntity catalogEntity =
        new CatalogEntity.Builder()
            .setName("testAwsConfigRoundTrip")
            .setDefaultBaseLocation(config.getAllowedLocations().getFirst())
            .setCatalogType(Catalog.TypeEnum.INTERNAL.name())
            .setStorageConfigurationInfo(
                realmConfig,
                MAPPER.readValue(configStr, StorageConfigInfo.class),
                config.getAllowedLocations().getFirst())
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
            .setStorageConfigurationInfo(realmConfig, storageConfigModel, baseLocation)
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

  public static Stream<Arguments> testAwsConfigRoundTrip() {
    AwsStorageConfigInfo.Builder b =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://example.com"))
            .setRoleArn("arn:aws:iam::012345678901:role/test-role");
    return Stream.of(
        Arguments.of(b.build()),
        Arguments.of(b.setExternalId("ex1").build()),
        Arguments.of(b.setRegion("us-west-2").build()),
        Arguments.of(b.setEndpoint("http://s3.example.com:1234").build()),
        Arguments.of(b.setStsEndpoint("http://sts.example.com:1234").build()),
        Arguments.of(b.setPathStyleAccess(true).build()));
  }
}

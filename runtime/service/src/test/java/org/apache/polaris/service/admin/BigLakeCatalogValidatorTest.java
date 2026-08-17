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
package org.apache.polaris.service.admin;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.GcpAuthenticationParameters;
import org.apache.polaris.core.admin.model.GcpStorageConfigInfo;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.service.TestServices;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BigLakeCatalogValidatorTest {
  private RealmConfig realmConfig;

  @BeforeEach
  void setup() {
    realmConfig = TestServices.builder().build().realmConfig();
  }

  @Test
  void validBigLakeConfigurationPasses() {
    assertThatCode(
            () ->
                BigLakeCatalogValidator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
                        "my-remote-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .doesNotThrowAnyException();
  }

  @Test
  void validBigLakeConfigurationWithLegacyCatalogQuotaProjectPasses() {
    CatalogProperties catalogProperties =
        CatalogProperties.builder("gs://bucket/path/to/data").build();
    catalogProperties.put("enable.credential.vending", "true");
    catalogProperties.put("header.x-goog-user-project", "my-billing-project");

    Catalog catalog =
        ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName("test-biglake-catalog")
            .setProperties(catalogProperties)
            .setStorageConfigInfo(validGcsStorage("gs://bucket/path/to/data"))
            .setConnectionConfigInfo(
                IcebergRestConnectionConfigInfo.builder()
                    .setConnectionType(ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
                    .setUri("https://biglake.googleapis.com/iceberg/v1/restcatalog")
                    .setRemoteCatalogName("my-remote-catalog")
                    .setProperties(Map.of())
                    .setAuthenticationParameters(
                        GcpAuthenticationParameters.builder()
                            .setAuthenticationType(
                                AuthenticationParameters.AuthenticationTypeEnum.GCP)
                            .build())
                    .build())
            .build();

    assertThatCode(() -> BigLakeCatalogValidator.validate(realmConfig, catalog))
        .doesNotThrowAnyException();
  }

  @Test
  void validBigLakeBlCatalogIdentifierPasses() {
    assertThatCode(
            () ->
                BigLakeCatalogValidator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
                        "bl://projects/123456789/catalogs/my-biglake-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .doesNotThrowAnyException();
  }

  @Test
  void validBigLakeWarehouseIdentifierPasses() {
    assertThatCode(
            () ->
                BigLakeCatalogValidator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
                        "gs://bucket/path/to/warehouse",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .doesNotThrowAnyException();
  }

  @Test
  void skipsValidationForNonBigLakeGcpRestEndpoint() {
    assertThatCode(
            () ->
                BigLakeCatalogValidator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://catalog-gateway.example.com/iceberg/v1",
                        null,
                        Map.of(),
                        true,
                        "s3://bucket/path/to/data",
                        null)))
        .doesNotThrowAnyException();
  }

  @Test
  void rejectsNonHttpsEndpoint() {
    assertThatThrownBy(
            () ->
                BigLakeCatalogValidator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "http://biglake.googleapis.com/iceberg/v1/restcatalog",
                        "my-remote-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires an https:// URI");
  }

  @Test
  void rejectsUnsupportedEndpointPath() {
    assertThatThrownBy(
            () ->
                BigLakeCatalogValidator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/not-biglake",
                        "my-remote-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unsupported path");
  }

  @Test
  void rejectsEndpointWithQueryString() {
    assertThatThrownBy(
            () ->
                BigLakeCatalogValidator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog?warehouse=test",
                        "my-remote-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("query, fragment, and custom port components are not supported");
  }

  @Test
  void rejectsEndpointWithFragment() {
    assertThatThrownBy(
            () ->
                BigLakeCatalogValidator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog#fragment",
                        "my-remote-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("query, fragment, and custom port components are not supported");
  }

  @Test
  void rejectsEndpointWithCustomPort() {
    assertThatThrownBy(
            () ->
                BigLakeCatalogValidator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com:8443/iceberg/v1/restcatalog",
                        "my-remote-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("query, fragment, and custom port components are not supported");
  }

  @Test
  void rejectsMissingRemoteCatalogIdentifier() {
    assertThatThrownBy(
            () ->
                BigLakeCatalogValidator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
                        " ",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("remote catalog or warehouse identifier is required");
  }

  @Test
  void rejectsMissingQuotaProjectHeader() {
    assertThatThrownBy(
            () ->
                BigLakeCatalogValidator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
                        "my-remote-catalog",
                        Map.of(),
                        true,
                        "gs://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("a quota project is required");
  }

  @Test
  void rejectsUnsupportedHeaderOverride() {
    assertThatThrownBy(
            () ->
                BigLakeCatalogValidator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
                        "my-remote-catalog",
                        Map.of(
                            "header.x-goog-user-project",
                            "my-billing-project",
                            "header.authorization",
                            "Bearer secret"),
                        true,
                        "gs://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("overriding security-sensitive headers is not allowed");
  }

  @Test
  void rejectsMissingGcsStorageWhenCredentialVendingEnabled() {
    assertThatThrownBy(
            () ->
                BigLakeCatalogValidator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
                        "my-remote-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        null)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("GCS storage configuration is required");
  }

  @Test
  void rejectsMalformedGsBaseLocation() {
    assertThatThrownBy(
            () ->
                BigLakeCatalogValidator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
                        "my-remote-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "s3://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("catalog.properties.default-base-location");
  }

  @Test
  void rejectsMalformedGsAllowedLocation() {
    assertThatThrownBy(
            () ->
                BigLakeCatalogValidator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
                        "my-remote-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        invalidAllowedLocationStorage())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("storageConfigInfo.allowedLocations[0]");
  }

  @Test
  void rejectsInvalidGcsServiceAccount() {
    assertThatThrownBy(
            () ->
                BigLakeCatalogValidator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
                        "my-remote-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        GcpStorageConfigInfo.builder()
                            .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
                            .setGcsServiceAccount("not-a-service-account")
                            .setAllowedLocations(List.of("gs://bucket/path/to/data"))
                            .build())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected a syntactically valid service account email");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "test-sa@my-project.iam.gserviceaccount.com",
        "123456789-compute@developer.gserviceaccount.com",
        "my-project@appspot.gserviceaccount.com"
      })
  void acceptsSupportedGcsServiceAccountEmailForms(String serviceAccount) {
    StorageConfigInfo storageConfigInfo =
        GcpStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
            .setGcsServiceAccount(serviceAccount)
            .setAllowedLocations(List.of("gs://bucket/path/to/data"))
            .build();

    assertThatCode(
            () ->
                BigLakeCatalogValidator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
                        "my-remote-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        storageConfigInfo)))
        .doesNotThrowAnyException();
  }

  private Catalog bigLakeCatalog(
      String uri,
      String remoteCatalogName,
      Map<String, String> connectionProperties,
      boolean credentialVendingEnabled,
      String defaultBaseLocation,
      StorageConfigInfo storageConfigInfo) {
    CatalogProperties catalogProperties = CatalogProperties.builder(defaultBaseLocation).build();
    catalogProperties.put("enable.credential.vending", Boolean.toString(credentialVendingEnabled));

    return ExternalCatalog.builder()
        .setType(Catalog.TypeEnum.EXTERNAL)
        .setName("test-biglake-catalog")
        .setProperties(catalogProperties)
        .setStorageConfigInfo(storageConfigInfo)
        .setConnectionConfigInfo(
            IcebergRestConnectionConfigInfo.builder()
                .setConnectionType(ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
                .setUri(uri)
                .setRemoteCatalogName(remoteCatalogName)
                .setProperties(connectionProperties)
                .setAuthenticationParameters(
                    GcpAuthenticationParameters.builder()
                        .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.GCP)
                        .build())
                .build())
        .build();
  }

  private StorageConfigInfo validGcsStorage(String allowedLocation) {
    return GcpStorageConfigInfo.builder()
        .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
        .setGcsServiceAccount("test-sa@my-project.iam.gserviceaccount.com")
        .setAllowedLocations(List.of(allowedLocation))
        .build();
  }

  private StorageConfigInfo invalidAllowedLocationStorage() {
    return GcpStorageConfigInfo.builder()
        .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
        .setGcsServiceAccount("test-sa@my-project.iam.gserviceaccount.com")
        .setAllowedLocations(List.of("bucket/path/to/data"))
        .build();
  }
}

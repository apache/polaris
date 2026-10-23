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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class BigLakeCatalogValidatorTest {
  private RealmConfig realmConfig;
  private final BigLakeCatalogValidator validator = new BigLakeCatalogValidator();

  @BeforeEach
  void setup() {
    realmConfig = TestServices.builder().build().realmConfig();
  }

  @Test
  void validBigLakeConfigurationPasses() {
    assertThatCode(
            () ->
                validator.validate(
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

    assertThatCode(() -> validator.validate(realmConfig, catalog)).doesNotThrowAnyException();
  }

  @Test
  void validBigLakeBlCatalogIdentifierPasses() {
    assertThatCode(
            () ->
                validator.validate(
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
  void validBigLakeBlCatalogIdentifierWithProjectIdPasses() {
    assertThatCode(
            () ->
                validator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
                        "bl://projects/my-project-id/catalogs/my-biglake-catalog",
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
                validator.validate(
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
                validator.validate(
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
  void skipsValidationForNonCanonicalEndpoints() {
    assertThatCode(
            () ->
                validator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "http://biglake.googleapis.com/iceberg/v1/restcatalog",
                        "my-remote-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .doesNotThrowAnyException();
  }

  @Test
  void skipsValidationForUnsupportedEndpointPath() {
    assertThatCode(
            () ->
                validator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/not-biglake",
                        "my-remote-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .doesNotThrowAnyException();
  }

  @Test
  void skipsValidationForEndpointWithQueryString() {
    assertThatCode(
            () ->
                validator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog?warehouse=test",
                        "my-remote-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .doesNotThrowAnyException();
  }

  @Test
  void skipsValidationForEndpointWithFragment() {
    assertThatCode(
            () ->
                validator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog#fragment",
                        "my-remote-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .doesNotThrowAnyException();
  }

  @Test
  void skipsValidationForEndpointWithCustomPort() {
    assertThatCode(
            () ->
                validator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com:8443/iceberg/v1/restcatalog",
                        "my-remote-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .doesNotThrowAnyException();
  }

  @Test
  void rejectsMissingRemoteCatalogIdentifier() {
    assertThatThrownBy(
            () ->
                validator.validate(
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
                validator.validate(
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

  @ParameterizedTest
  @ValueSource(strings = {"HEADER.X-GOOG-USER-PROJECT", "header.X-Goog-User-Project"})
  void rejectsNonCanonicalConnectionQuotaProjectKey(String propertyName) {
    assertThatThrownBy(
            () ->
                validator.validate(
                    realmConfig,
                    bigLakeCatalogWithProperties(
                        Map.of(propertyName, "connection-project"), Map.of())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("header.x-goog-user-project");
  }

  @ParameterizedTest
  @ValueSource(strings = {"HEADER.X-GOOG-USER-PROJECT", "Header.x-goog-user-project"})
  void rejectsNonCanonicalCatalogQuotaProjectPrefix(String propertyName) {
    assertThatThrownBy(
            () ->
                validator.validate(
                    realmConfig,
                    bigLakeCatalogWithProperties(
                        Map.of(), Map.of(propertyName, "catalog-project"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("header.x-goog-user-project");
  }

  @ParameterizedTest
  @ValueSource(strings = {"header.X-Goog-User-Project", "header.X-GOOG-USER-PROJECT"})
  void acceptsCatalogQuotaProjectWithCaseInsensitiveHeaderName(String propertyName) {
    assertThatCode(
            () ->
                validator.validate(
                    realmConfig,
                    bigLakeCatalogWithProperties(
                        Map.of(), Map.of(propertyName, "catalog-project"))))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "\t", "other-project"})
  void acceptsCanonicalCatalogQuotaProjectOverCaseVariant(String caseVariantValue) {
    Map<String, String> catalogProperties = new HashMap<>();
    catalogProperties.put("header.x-goog-user-project", "canonical-project");
    catalogProperties.put("header.X-Goog-User-Project", caseVariantValue);

    assertThatCode(
            () ->
                validator.validate(
                    realmConfig, bigLakeCatalogWithProperties(Map.of(), catalogProperties)))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "\t"})
  void rejectsBlankCanonicalCatalogQuotaProjectDespiteCaseVariant(String canonicalValue) {
    Map<String, String> catalogProperties = new HashMap<>();
    catalogProperties.put("header.x-goog-user-project", canonicalValue);
    catalogProperties.put("header.X-Goog-User-Project", "other-project");

    assertThatThrownBy(
            () ->
                validator.validate(
                    realmConfig, bigLakeCatalogWithProperties(Map.of(), catalogProperties)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("header.x-goog-user-project");
  }

  @Test
  void rejectsBlankConnectionQuotaProjectOverBothCatalogCaseVariants() {
    assertThatThrownBy(
            () ->
                validator.validate(
                    realmConfig,
                    bigLakeCatalogWithProperties(
                        Map.of("header.x-goog-user-project", ""),
                        Map.of(
                            "header.x-goog-user-project", "canonical-project",
                            "header.X-Goog-User-Project", "other-project"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("header.x-goog-user-project");
  }

  @Test
  void acceptsConnectionQuotaProjectOverBothCatalogCaseVariants() {
    assertThatCode(
            () ->
                validator.validate(
                    realmConfig,
                    bigLakeCatalogWithProperties(
                        Map.of("header.x-goog-user-project", "connection-project"),
                        Map.of(
                            "header.x-goog-user-project", "",
                            "header.X-Goog-User-Project", "other-project"))))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @MethodSource("blankConnectionQuotaProjectOverrides")
  void rejectsBlankConnectionQuotaProjectOverride(String quotaProject, String catalogPropertyName) {
    Map<String, String> connectionProperties = new HashMap<>();
    connectionProperties.put("header.x-goog-user-project", quotaProject);

    assertThatThrownBy(
            () ->
                validator.validate(
                    realmConfig,
                    bigLakeCatalogWithProperties(
                        connectionProperties, Map.of(catalogPropertyName, "catalog-project"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("header.x-goog-user-project");
  }

  @ParameterizedTest
  @ValueSource(strings = {"HEADER.X-GOOG-USER-PROJECT", "header.X-Goog-User-Project"})
  void ignoresNonCanonicalConnectionQuotaProjectOverride(String propertyName) {
    assertThatCode(
            () ->
                validator.validate(
                    realmConfig,
                    bigLakeCatalogWithProperties(
                        Map.of(propertyName, " "),
                        Map.of("header.x-goog-user-project", "catalog-project"))))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(strings = {"header.x-goog-user-project", "header.X-Goog-User-Project"})
  void acceptsConnectionQuotaProjectOverBlankCatalogQuotaProject(String catalogPropertyName) {
    assertThatCode(
            () ->
                validator.validate(
                    realmConfig,
                    bigLakeCatalogWithProperties(
                        Map.of("header.x-goog-user-project", "connection-project"),
                        Map.of(catalogPropertyName, " "))))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsCatalogQuotaProjectWithoutConnectionProperties() {
    assertThatCode(
            () ->
                validator.validate(
                    realmConfig,
                    bigLakeCatalogWithProperties(
                        null, Map.of("header.x-goog-user-project", "catalog-project"))))
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsOtherHeaderProperties() {
    assertThatCode(
            () ->
                validator.validate(
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
        .doesNotThrowAnyException();
  }

  @Test
  void acceptsAdditionalHeaderProperties() {
    assertThatCode(
            () ->
                validator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
                        "my-remote-catalog",
                        Map.of(
                            "header.x-goog-user-project",
                            "my-billing-project",
                            "header.x-custom-header",
                            "value"),
                        true,
                        "gs://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .doesNotThrowAnyException();
  }

  @Test
  void rejectsMissingGcsStorageWhenCredentialVendingEnabled() {
    assertThatThrownBy(
            () ->
                validator.validate(
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
  void requiresBaseLocationButDoesNotValidateItsFormat() {
    assertThatCode(
            () ->
                validator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
                        "my-remote-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "s3://bucket/path/to/data",
                        validGcsStorage("gs://bucket/path/to/data"))))
        .doesNotThrowAnyException();
  }

  @Test
  void doesNotValidateAllowedLocationFormat() {
    assertThatCode(
            () ->
                validator.validate(
                    realmConfig,
                    bigLakeCatalog(
                        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
                        "my-remote-catalog",
                        Map.of("header.x-goog-user-project", "my-billing-project"),
                        true,
                        "gs://bucket/path/to/data",
                        invalidAllowedLocationStorage())))
        .doesNotThrowAnyException();
  }

  @Test
  void requiresServiceAccountButDoesNotValidateItsFormat() {
    assertThatCode(
            () ->
                validator.validate(
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
        .doesNotThrowAnyException();
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
                validator.validate(
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

  private static Stream<Arguments> blankConnectionQuotaProjectOverrides() {
    return Stream.of(null, "", " ", "\t")
        .flatMap(
            quotaProject ->
                Stream.of("header.x-goog-user-project", "header.X-Goog-User-Project")
                    .map(propertyName -> Arguments.of(quotaProject, propertyName)));
  }

  private Catalog bigLakeCatalogWithProperties(
      Map<String, String> connectionProperties, Map<String, String> catalogProperties) {
    Catalog catalog =
        bigLakeCatalog(
            "https://biglake.googleapis.com/iceberg/v1/restcatalog",
            "my-remote-catalog",
            connectionProperties,
            false,
            "gs://bucket/path/to/data",
            null);
    catalogProperties.forEach(catalog.getProperties()::put);
    return catalog;
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

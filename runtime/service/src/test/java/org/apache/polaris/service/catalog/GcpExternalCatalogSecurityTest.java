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
package org.apache.polaris.service.catalog;

import static org.assertj.core.api.Assertions.assertThat;
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
import org.apache.polaris.core.admin.model.OAuthClientCredentialsParameters;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.junit.jupiter.api.Test;

public class GcpExternalCatalogSecurityTest {
  @Test
  public void validateNoSensitivePropertiesRejectsSensitiveGooglePropertyKey() {
    Catalog catalog =
        gcpCatalog(
            CatalogProperties.builder("gs://bucket/path/to/data")
                .addProperty(
                    "google_application_credentials", "/var/run/secrets/google/key.json")
                .build(),
            Map.of("header.x-goog-user-project", "billing-project"));

    assertThatThrownBy(() -> GcpExternalCatalogSecurity.validateNoSensitiveProperties(catalog))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("catalog.properties.google_application_credentials")
        .hasMessageContaining("Application Default Credentials");
  }

  @Test
  public void validateNoSensitivePropertiesRejectsSensitiveGooglePropertyValue() {
    Catalog catalog =
        gcpCatalog(
            CatalogProperties.builder("gs://bucket/path/to/data").build(),
            Map.of(
                "header.x-goog-user-project",
                "billing-project",
                "custom-header",
                "{\"type\": \"service_account\", \"private_key\": \"test\"}"));

    assertThatThrownBy(() -> GcpExternalCatalogSecurity.validateNoSensitiveProperties(catalog))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("connectionConfigInfo.properties.custom-header")
        .hasMessageContaining("GOOGLE_APPLICATION_CREDENTIALS");
  }

  @Test
  public void sanitizeCatalogRemovesSensitivePropertiesFromResponseModel() {
    Catalog catalog =
        gcpCatalog(
            CatalogProperties.builder("gs://bucket/path/to/data")
                .addProperty("google.credentials.path", "/var/run/secrets/google/key.json")
                .addProperty("safe-property", "safe-value")
                .build(),
            Map.of(
                "header.x-goog-user-project",
                "billing-project",
                "x-extra-header",
                "safe-value",
                "gcp.access-token",
                "ya29.test-token"));

    ExternalCatalog sanitizedCatalog =
        (ExternalCatalog) GcpExternalCatalogSecurity.sanitizeCatalog(catalog);
    IcebergRestConnectionConfigInfo sanitizedConnectionConfig =
        (IcebergRestConnectionConfigInfo) sanitizedCatalog.getConnectionConfigInfo();

    assertThat(sanitizedCatalog.getProperties().toMap())
        .containsEntry("default-base-location", "gs://bucket/path/to/data")
        .containsEntry("safe-property", "safe-value")
        .doesNotContainKey("google.credentials.path");
    assertThat(sanitizedConnectionConfig.getProperties())
        .containsEntry("header.x-goog-user-project", "billing-project")
        .containsEntry("x-extra-header", "safe-value")
        .doesNotContainKey("gcp.access-token");
  }

  @Test
  public void sanitizeCatalogLeavesNonGcpCatalogUnchanged() {
    Catalog catalog =
        ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName("oauth-catalog")
            .setProperties(
                CatalogProperties.builder("gs://bucket/path/to/data")
                    .addProperty("google.credentials.path", "/var/run/secrets/google/key.json")
                    .build())
            .setStorageConfigInfo(
                GcpStorageConfigInfo.builder()
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
                    .setAllowedLocations(List.of("gs://bucket/path/to/data"))
                    .build())
            .setConnectionConfigInfo(
                IcebergRestConnectionConfigInfo.builder(
                        ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
                    .setUri("https://example.com/catalog")
                    .setRemoteCatalogName("remote-catalog")
                    .setProperties(
                        Map.of("google.credentials.path", "/var/run/secrets/google/key.json"))
                    .setAuthenticationParameters(
                        OAuthClientCredentialsParameters.builder(
                                AuthenticationParameters.AuthenticationTypeEnum.OAUTH)
                            .setClientId("client-id")
                            .setClientSecret("client-secret")
                            .build())
                    .build())
            .build();

    assertThat(GcpExternalCatalogSecurity.sanitizeCatalog(catalog)).isSameAs(catalog);
  }

  private Catalog gcpCatalog(
      CatalogProperties catalogProperties, Map<String, String> connectionProperties) {
    return ExternalCatalog.builder()
        .setType(Catalog.TypeEnum.EXTERNAL)
        .setName("biglake-catalog")
        .setProperties(catalogProperties)
        .setStorageConfigInfo(
            GcpStorageConfigInfo.builder()
                .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
                .setAllowedLocations(List.of("gs://bucket/path/to/data"))
                .build())
        .setConnectionConfigInfo(
            IcebergRestConnectionConfigInfo.builder(
                    ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
                .setUri("https://biglake.googleapis.com/iceberg/v1/restcatalog")
                .setRemoteCatalogName("remote-catalog")
                .setProperties(connectionProperties)
                .setAuthenticationParameters(
                    GcpAuthenticationParameters.builder()
                        .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.GCP)
                        .build())
                .build())
        .build();
  }
}

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
package org.apache.polaris.core.connection.bigquery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.polaris.core.connection.ImplicitAuthenticationParametersDpo;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.junit.jupiter.api.Test;

class BigQueryMetastoreConnectionConfigInfoDpoTest {

  @Test
  void testAllowedConnectionPropertiesAreForwarded() {
    BigQueryMetastoreConnectionConfigInfoDpo dpo =
        createDpo(
            Map.of(
                "gcp.bigquery.project-id",
                "my-gcp-project",
                "warehouse",
                "analytics_dataset",
                "gcp.bigquery.location",
                "US",
                "gcp.bigquery.list-all-tables",
                "false",
                "gcp.bigquery.impersonate.service-account",
                "sa@example.iam.gserviceaccount.com",
                "gcp.bigquery.impersonate.lifetime-seconds",
                "3600",
                "gcp.bigquery.impersonate.scopes",
                "https://www.googleapis.com/auth/bigquery",
                "gcp.bigquery.impersonate.delegates",
                "delegate@example.iam.gserviceaccount.com"));

    Map<String, String> properties = dpo.asIcebergCatalogProperties(mockCredentialManager(dpo));

    assertThat(properties)
        .containsEntry("gcp.bigquery.project-id", "my-gcp-project")
        .containsEntry("warehouse", "analytics_dataset")
        .containsEntry("gcp.bigquery.location", "US")
        .containsEntry("gcp.bigquery.list-all-tables", "false")
        .containsEntry(
            "gcp.bigquery.impersonate.service-account", "sa@example.iam.gserviceaccount.com")
        .containsEntry("gcp.bigquery.impersonate.lifetime-seconds", "3600")
        .containsEntry(
            "gcp.bigquery.impersonate.scopes", "https://www.googleapis.com/auth/bigquery")
        .containsEntry(
            "gcp.bigquery.impersonate.delegates", "delegate@example.iam.gserviceaccount.com");
  }

  @Test
  void testOtherConnectionPropertiesAreNotForwarded() {
    BigQueryMetastoreConnectionConfigInfoDpo dpo =
        createDpo(
            Map.of(
                "gcp.bigquery.project-id",
                "my-gcp-project",
                "warehouse",
                "analytics_dataset",
                CatalogProperties.FILE_IO_IMPL,
                "com.example.Custom",
                CatalogProperties.LOCK_IMPL,
                "com.example.Custom",
                "gcs.oauth2.token",
                "ya29...",
                "custom-key",
                "custom-value"));

    Map<String, String> properties = dpo.asIcebergCatalogProperties(mockCredentialManager(dpo));

    assertThat(properties)
        .containsEntry("gcp.bigquery.project-id", "my-gcp-project")
        .containsEntry("warehouse", "analytics_dataset")
        .doesNotContainKeys(
            CatalogProperties.FILE_IO_IMPL,
            CatalogProperties.LOCK_IMPL,
            "gcs.oauth2.token",
            "custom-key");
  }

  @Test
  void testNullConnectionPropertiesHandledGracefully() {
    BigQueryMetastoreConnectionConfigInfoDpo dpo = createDpo(null);

    Map<String, String> properties = dpo.asIcebergCatalogProperties(mockCredentialManager(dpo));

    assertThat(properties)
        .doesNotContainKeys(CatalogProperties.FILE_IO_IMPL, "gcp.bigquery.project-id", "warehouse");
  }

  @Test
  void testConnectionCredentialsAreStillForwarded() {
    BigQueryMetastoreConnectionConfigInfoDpo dpo =
        createDpo(Map.of("gcp.bigquery.project-id", "my-gcp-project", "warehouse", "ds"));
    PolarisCredentialManager credentialManager = mock(PolarisCredentialManager.class);
    ConnectionCredentials credentials = mock(ConnectionCredentials.class);
    when(credentials.credentials()).thenReturn(Map.of("credential_key", "credential_value"));
    when(credentialManager.getConnectionCredentials(dpo)).thenReturn(credentials);

    Map<String, String> properties = dpo.asIcebergCatalogProperties(credentialManager);

    assertThat(properties).containsEntry("credential_key", "credential_value");
  }

  private static PolarisCredentialManager mockCredentialManager(
      BigQueryMetastoreConnectionConfigInfoDpo dpo) {
    PolarisCredentialManager credentialManager = mock(PolarisCredentialManager.class);
    ConnectionCredentials credentials = mock(ConnectionCredentials.class);
    when(credentials.credentials()).thenReturn(Map.of());
    when(credentialManager.getConnectionCredentials(dpo)).thenReturn(credentials);
    return credentialManager;
  }

  private static BigQueryMetastoreConnectionConfigInfoDpo createDpo(
      Map<String, String> properties) {
    return new BigQueryMetastoreConnectionConfigInfoDpo(
        BigQueryMetastoreConnectionConfigInfoDpo.DEFAULT_URI,
        new ImplicitAuthenticationParametersDpo(),
        properties,
        null);
  }
}

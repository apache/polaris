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
package org.apache.polaris.core.connection.hive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.polaris.core.connection.ImplicitAuthenticationParametersDpo;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.junit.jupiter.api.Test;

class HiveConnectionConfigInfoDpoTest {

  @Test
  void testAllowedConnectionPropertiesAreForwarded() {
    HiveConnectionConfigInfoDpo dpo =
        createDpo(
            Map.of(
                CatalogProperties.CLIENT_POOL_SIZE, "5",
                S3FileIOProperties.ENDPOINT, "http://minio:9000",
                S3FileIOProperties.PATH_STYLE_ACCESS, "true"));

    Map<String, String> properties = dpo.asIcebergCatalogProperties(mockCredentialManager(dpo));

    assertThat(properties)
        .containsEntry(CatalogProperties.CLIENT_POOL_SIZE, "5")
        .containsEntry(S3FileIOProperties.ENDPOINT, "http://minio:9000")
        .containsEntry(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
  }

  @Test
  void testOtherConnectionPropertiesAreNotForwarded() {
    HiveConnectionConfigInfoDpo dpo =
        createDpo(
            Map.of(
                CatalogProperties.FILE_IO_IMPL,
                "com.example.Custom",
                "s3.access-key-id",
                "AKIA...",
                "custom-key",
                "custom-value"));

    Map<String, String> properties = dpo.asIcebergCatalogProperties(mockCredentialManager(dpo));

    assertThat(properties)
        .doesNotContainKeys(CatalogProperties.FILE_IO_IMPL, "s3.access-key-id", "custom-key");
  }

  @Test
  void testNullConnectionPropertiesHandledGracefully() {
    HiveConnectionConfigInfoDpo dpo = createDpo(null);

    Map<String, String> properties = dpo.asIcebergCatalogProperties(mockCredentialManager(dpo));

    assertThat(properties).containsEntry(CatalogProperties.URI, dpo.getUri());
  }

  private static PolarisCredentialManager mockCredentialManager(HiveConnectionConfigInfoDpo dpo) {
    PolarisCredentialManager credentialManager = mock(PolarisCredentialManager.class);
    ConnectionCredentials credentials = mock(ConnectionCredentials.class);
    when(credentials.credentials()).thenReturn(Map.of());
    when(credentialManager.getConnectionCredentials(dpo)).thenReturn(credentials);
    return credentialManager;
  }

  private static HiveConnectionConfigInfoDpo createDpo(Map<String, String> properties) {
    return new HiveConnectionConfigInfoDpo(
        "thrift://hms:9083",
        new ImplicitAuthenticationParametersDpo(),
        "s3://bucket/warehouse",
        null,
        properties);
  }
}

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
package org.apache.polaris.core.connection.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ImplicitAuthenticationParametersDpo;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class HadoopConnectionConfigInfoDpoTest {

  @Test
  void testAllowedConnectionPropertiesAreForwarded() {
    HadoopConnectionConfigInfoDpo dpo =
        createDpo(
            Map.of(
                "suppress-permission-error",
                "true",
                S3FileIOProperties.ENDPOINT,
                "http://minio:9000",
                S3FileIOProperties.PATH_STYLE_ACCESS,
                "true",
                AwsClientProperties.CLIENT_REGION,
                "us-east-1"));
    Map<String, String> properties = dpo.asIcebergCatalogProperties(mockCredentialManager(dpo));
    assertThat(properties)
        .containsEntry("suppress-permission-error", "true")
        .containsEntry(S3FileIOProperties.ENDPOINT, "http://minio:9000")
        .containsEntry(S3FileIOProperties.PATH_STYLE_ACCESS, "true")
        .containsEntry(AwsClientProperties.CLIENT_REGION, "us-east-1");
  }

  @Test
  void testOtherConnectionPropertiesAreNotForwarded() {
    HadoopConnectionConfigInfoDpo dpo =
        createDpo(
            Map.of(
                CatalogProperties.FILE_IO_IMPL,
                "com.example.Custom",
                CatalogProperties.LOCK_IMPL,
                "com.example.Custom",
                "s3.access-key-id",
                "AKIA...",
                "custom-key",
                "custom-value"));

    Map<String, String> properties = dpo.asIcebergCatalogProperties(mockCredentialManager(dpo));

    assertThat(properties)
        .doesNotContainKeys(
            CatalogProperties.FILE_IO_IMPL,
            CatalogProperties.LOCK_IMPL,
            "s3.access-key-id",
            "custom-key");
  }

  @Test
  void testNullConnectionPropertiesHandledGracefully() {
    HadoopConnectionConfigInfoDpo dpo = createDpo(null);

    Map<String, String> properties = dpo.asIcebergCatalogProperties(mockCredentialManager(dpo));

    assertThat(properties).containsEntry(CatalogProperties.URI, dpo.getUri());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "hdfs://namenode:8020/warehouse",
        "s3://bucket/warehouse",
        "abfss://container@account.dfs.core.windows.net/warehouse"
      })
  void testUriWithoutUrlHandlerIsAccepted(String uri) {
    // The Hadoop catalog never connects to `uri`, so any URI is fine, including schemes the JVM
    // has no URL handler for.
    assertThatCode(() -> createDpo(uri, null)).doesNotThrowAnyException();
  }

  @Test
  void testUriIsOptional() {
    HadoopConnectionConfigInfoDpo dpo = createDpo(null, null);

    assertThat(dpo.getUri()).isNull();
    assertThat(dpo.asIcebergCatalogProperties(mockCredentialManager(dpo)))
        .doesNotContainKey(CatalogProperties.URI)
        .containsEntry(CatalogProperties.WAREHOUSE_LOCATION, "warehouse");

    ConnectionConfigInfo model = dpo.asConnectionConfigInfoModel(null);
    assertThat(model.getUri()).isNull();

    ConnectionConfigInfoDpo roundTrip = ConnectionConfigInfoDpo.deserialize(dpo.serialize());
    assertThat(roundTrip).isInstanceOf(HadoopConnectionConfigInfoDpo.class);
    assertThat(roundTrip.getUri()).isNull();
    assertThat(((HadoopConnectionConfigInfoDpo) roundTrip).getWarehouse()).isEqualTo("warehouse");
  }

  private static PolarisCredentialManager mockCredentialManager(HadoopConnectionConfigInfoDpo dpo) {
    PolarisCredentialManager credentialManager = mock(PolarisCredentialManager.class);
    ConnectionCredentials credentials = mock(ConnectionCredentials.class);
    when(credentials.credentials()).thenReturn(Map.of());
    when(credentialManager.getConnectionCredentials(dpo)).thenReturn(credentials);
    return credentialManager;
  }

  private static HadoopConnectionConfigInfoDpo createDpo(Map<String, String> properties) {
    return createDpo("file:///warehouse", properties);
  }

  private static HadoopConnectionConfigInfoDpo createDpo(
      String uri, Map<String, String> properties) {
    return new HadoopConnectionConfigInfoDpo(
        uri, new ImplicitAuthenticationParametersDpo(), null, "warehouse", properties);
  }
}

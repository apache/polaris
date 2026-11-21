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
package org.apache.polaris.service.catalog.io;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.service.storage.aws.S3AccessConfig;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

/**
 * Verifies AccessConfig overlays contextual properties, and documents the trusted-context contract
 * for server FileIO construction.
 */
public class DefaultFileIOFactoryAccessConfigTest {

  @Test
  void accessConfigEndpointOverridesContextualProperties() {
    String catalogEndpoint = "https://catalog-storage.example";
    String contextualEndpoint = "https://table-default-endpoint.example";

    StorageAccessConfig accessConfig =
        StorageAccessConfig.builder()
            .putCredential(S3FileIOProperties.ACCESS_KEY_ID, "ak")
            .putCredential(S3FileIOProperties.SECRET_ACCESS_KEY, "sk")
            .putCredential(S3FileIOProperties.SESSION_TOKEN, "token")
            .putExtraProperty(StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), catalogEndpoint)
            .supportsCredentialVending(true)
            .build();

    // Simulates catalog table-default.* (trusted), not metadata.properties().
    Map<String, String> contextualProperties =
        Map.of(StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), contextualEndpoint);

    AtomicReference<Map<String, String>> captured = new AtomicReference<>();
    DefaultFileIOFactory factory =
        new DefaultFileIOFactory(S3AccessConfig.empty()) {
          @Override
          FileIO loadFileIOInternal(
              @NonNull String ioImplClassName, @NonNull Map<String, String> properties) {
            captured.set(Map.copyOf(properties));
            return new InMemoryFileIO();
          }
        };

    factory.loadFileIO(accessConfig, InMemoryFileIO.class.getName(), contextualProperties);

    assertThat(captured.get())
        .containsEntry(StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), catalogEndpoint)
        .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, "ak")
        .containsEntry(S3FileIOProperties.SECRET_ACCESS_KEY, "sk")
        .containsEntry(S3FileIOProperties.SESSION_TOKEN, "token");
  }

  @Test
  void catalogTrustedContextAppliesWhenAccessConfigHasNoEndpoint() {
    // Matches Spark IT / SKIP_CREDENTIAL_SUBSCOPING: empty AccessConfig, MinIO settings come from
    // catalog-trusted contextual properties (e.g. table-default.*).
    StorageAccessConfig accessConfig =
        StorageAccessConfig.builder().supportsCredentialVending(false).build();

    String endpoint = "http://127.0.0.1:9000";
    Map<String, String> contextualProperties =
        Map.of(
            StorageAccessProperty.AWS_ENDPOINT.getPropertyName(),
            endpoint,
            StorageAccessProperty.AWS_PATH_STYLE_ACCESS.getPropertyName(),
            "true",
            S3FileIOProperties.ACCESS_KEY_ID,
            "polaris-access",
            S3FileIOProperties.SECRET_ACCESS_KEY,
            "polaris-secret");

    AtomicReference<Map<String, String>> captured = new AtomicReference<>();
    DefaultFileIOFactory factory =
        new DefaultFileIOFactory(S3AccessConfig.empty()) {
          @Override
          FileIO loadFileIOInternal(
              @NonNull String ioImplClassName, @NonNull Map<String, String> properties) {
            captured.set(Map.copyOf(properties));
            return new InMemoryFileIO();
          }
        };

    factory.loadFileIO(accessConfig, InMemoryFileIO.class.getName(), contextualProperties);

    assertThat(captured.get())
        .containsEntry(StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), endpoint)
        .containsEntry(StorageAccessProperty.AWS_PATH_STYLE_ACCESS.getPropertyName(), "true")
        .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, "polaris-access")
        .containsEntry(S3FileIOProperties.SECRET_ACCESS_KEY, "polaris-secret");
  }
}

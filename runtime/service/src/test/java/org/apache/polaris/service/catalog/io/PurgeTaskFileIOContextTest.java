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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.service.task.TaskFileIOSupplier;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

/**
 * Follow-up to excluding caller metadata from server FileIO (#5634): purge tasks must still carry
 * catalog-trusted table-default.* settings so TaskFileIOSupplier can rebuild FileIO when
 * AccessConfig is empty (SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION), without reintroducing caller
 * metadata FileIO client keys.
 */
public class PurgeTaskFileIOContextTest {

  private static final TableIdentifier TABLE = TableIdentifier.of("ns", "t1");
  private static final String TRUSTED_ENDPOINT = "http://trusted-minio:9000";
  private static final String CALLER_ENDPOINT = "http://attacker.example";

  @Test
  void purgeTaskMapKeepsCatalogDefaultsAndExcludesCallerMetadata() {
    Map<String, String> tableDefaultProperties =
        Map.of(
            StorageAccessProperty.AWS_ENDPOINT.getPropertyName(),
            TRUSTED_ENDPOINT,
            StorageAccessProperty.AWS_PATH_STYLE_ACCESS.getPropertyName(),
            "true",
            "s3.access-key-id",
            "trusted-key",
            "s3.secret-access-key",
            "trusted-secret");

    Map<String, String> callerMetadataProperties =
        Map.of(
            StorageAccessProperty.AWS_ENDPOINT.getPropertyName(),
            CALLER_ENDPOINT,
            "s3.access-key-id",
            "attacker-key");

    Map<String, String> storageEntityInternal =
        Map.of("polaris.storage.aws.role-arn", "arn:aws:iam::1:role/r");

    // Mirrors LocalIcebergCatalog.dropTable/dropView purge task construction after #5634 follow-up.
    Map<String, String> purgeTaskProperties = new HashMap<>();
    purgeTaskProperties.putAll(tableDefaultProperties);
    purgeTaskProperties.put(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName());
    purgeTaskProperties.putAll(storageEntityInternal);
    purgeTaskProperties.put(PolarisTaskConstants.STORAGE_LOCATION, "s3://bucket/ns/t1");
    // Caller metadata must never be merged into the task map.
    assertThat(purgeTaskProperties)
        .containsEntry(StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), TRUSTED_ENDPOINT)
        .containsEntry("s3.access-key-id", "trusted-key")
        .doesNotContainEntry(StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), CALLER_ENDPOINT);
    assertThat(callerMetadataProperties)
        .containsEntry(StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), CALLER_ENDPOINT);

    TaskEntity task =
        new TaskEntity.Builder()
            .setId(1L)
            .setName("purge-task")
            .setCreateTimestamp(1L)
            .setInternalProperties(purgeTaskProperties)
            .build();

    StorageAccessConfig emptyAccessConfig =
        StorageAccessConfig.builder().supportsCredentialVending(false).build();
    StorageAccessConfigProvider accessConfigProvider = mock(StorageAccessConfigProvider.class);
    when(accessConfigProvider.getStorageAccessConfig(any(), any(), any(), any(), any()))
        .thenReturn(emptyAccessConfig);

    AtomicReference<Map<String, String>> capturedFileIOProperties = new AtomicReference<>();
    FileIOFactory capturingFactory =
        new FileIOFactory() {
          @Override
          public FileIO loadFileIO(
              @NonNull StorageAccessConfig storageAccessConfig,
              @NonNull String ioImplClassName,
              @NonNull Map<String, String> properties) {
            capturedFileIOProperties.set(Map.copyOf(properties));
            return new InMemoryFileIO();
          }
        };

    FileIO fileIO =
        new TaskFileIOSupplier(capturingFactory, accessConfigProvider).apply(task, TABLE);

    assertThat(fileIO).isInstanceOf(InMemoryFileIO.class);
    assertThat(capturedFileIOProperties.get())
        .containsEntry(StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), TRUSTED_ENDPOINT)
        .containsEntry("s3.access-key-id", "trusted-key")
        .containsEntry(PolarisTaskConstants.STORAGE_LOCATION, "s3://bucket/ns/t1")
        .doesNotContainEntry(StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), CALLER_ENDPOINT)
        .doesNotContainEntry("s3.access-key-id", "attacker-key");
  }
}

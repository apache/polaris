/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.junit.jupiter.api.Test;

class TaskFileIOSupplierTest {
  @Test
  void closesRawFileIoWhenTaskEncryptionInitializationFails() {
    FileIO fileIO = mock(FileIO.class);
    RuntimeException closeFailure = new RuntimeException("close failure");
    doThrow(closeFailure).when(fileIO).close();

    StorageAccessConfigProvider storageAccessConfigProvider =
        mock(StorageAccessConfigProvider.class);
    when(storageAccessConfigProvider.getStorageAccessConfig(any(), any(), any(), any(), any()))
        .thenReturn(StorageAccessConfig.builder().build());
    FileIOFactory fileIOFactory = (accessConfig, ioImplClassName, properties) -> fileIO;
    TaskFileIOSupplier supplier =
        new TaskFileIOSupplier(fileIOFactory, storageAccessConfigProvider);
    TaskEntity task =
        new TaskEntity.Builder()
            .setName("encrypted-cleanup")
            .setInternalProperties(
                Map.of(
                    PolarisTaskConstants.STORAGE_LOCATION,
                    "file:///tmp/encrypted-cleanup",
                    TableProperties.ENCRYPTION_TABLE_KEY,
                    "keyA"))
            .build();

    assertThatThrownBy(() -> supplier.apply(task, TableIdentifier.of(Namespace.of("ns"), "table")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Missing encryption context")
        .satisfies(failure -> assertThat(failure.getSuppressed()).containsExactly(closeFailure));
  }

  @Test
  void doesNotExposeEncryptionContextToFileIo() {
    FileIO fileIO = mock(FileIO.class);
    StorageAccessConfigProvider storageAccessConfigProvider =
        mock(StorageAccessConfigProvider.class);
    when(storageAccessConfigProvider.getStorageAccessConfig(any(), any(), any(), any(), any()))
        .thenReturn(StorageAccessConfig.builder().build());
    AtomicReference<Map<String, String>> loadedProperties = new AtomicReference<>();
    FileIOFactory fileIOFactory =
        (accessConfig, ioImplClassName, properties) -> {
          loadedProperties.set(properties);
          return fileIO;
        };
    TaskFileIOSupplier supplier =
        new TaskFileIOSupplier(fileIOFactory, storageAccessConfigProvider);
    TaskEntity task =
        new TaskEntity.Builder()
            .setName("cleanup")
            .setInternalProperties(
                Map.of(
                    PolarisTaskConstants.STORAGE_LOCATION,
                    "file:///tmp/cleanup",
                    PolarisTaskConstants.ENCRYPTION_CONTEXT,
                    "opaque-task-context"))
            .build();

    assertThat(supplier.apply(task, TableIdentifier.of(Namespace.of("ns"), "table")))
        .isSameAs(fileIO);
    assertThat(loadedProperties.get())
        .containsEntry(PolarisTaskConstants.STORAGE_LOCATION, "file:///tmp/cleanup")
        .doesNotContainKey(PolarisTaskConstants.ENCRYPTION_CONTEXT);
  }
}

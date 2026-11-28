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
package org.apache.polaris.service.task;

import static org.apache.polaris.service.task.TaskTestUtils.TASK_LOCATION;
import static org.apache.polaris.service.task.TaskTestUtils.addTaskLocation;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TaskFileIOSupplierTest {

  @Test
  @SuppressWarnings("unchecked")
  void cleanupTasksRequestReadListAndDeleteButNotWrite() {
    // Cleanup tasks read metadata and delete files. Not asking for WRITE keeps storage
    // integrations from preparing for writes, e.g. creating GCS parent folders.
    StorageAccessConfigProvider accessConfigProvider =
        Mockito.mock(StorageAccessConfigProvider.class);
    Mockito.when(
            accessConfigProvider.getStorageAccessConfig(
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(StorageAccessConfig.builder().build());
    FileIOFactory fileIOFactory = Mockito.mock(FileIOFactory.class);
    TaskEntity task =
        addTaskLocation(
            new TaskEntity.Builder()
                .withTaskType(AsyncTaskType.BATCH_FILE_CLEANUP)
                .setName(UUID.randomUUID().toString())
                .build());
    TableIdentifier table = TableIdentifier.of(Namespace.of("db"), "tbl");

    new TaskFileIOSupplier(fileIOFactory, accessConfigProvider).apply(task, table);

    ArgumentCaptor<Set<PolarisStorageActions>> actions = ArgumentCaptor.forClass(Set.class);
    Mockito.verify(accessConfigProvider)
        .getStorageAccessConfig(
            Mockito.eq(table),
            Mockito.eq(Set.of(TASK_LOCATION)),
            actions.capture(),
            Mockito.any(),
            Mockito.any());
    assertThat(actions.getValue())
        .containsExactlyInAnyOrder(
            PolarisStorageActions.READ, PolarisStorageActions.LIST, PolarisStorageActions.DELETE);
  }
}

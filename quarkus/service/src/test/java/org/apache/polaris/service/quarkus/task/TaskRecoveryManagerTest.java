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
package org.apache.polaris.service.quarkus.task;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.annotation.Nonnull;
import jakarta.inject.Inject;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.task.TableCleanupTaskHandler;
import org.apache.polaris.service.task.TaskExecutorImpl;
import org.apache.polaris.service.task.TaskFileIOSupplier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.threeten.extra.MutableClock;

@QuarkusTest
public class TaskRecoveryManagerTest {
  @Inject private MetaStoreManagerFactory metaStoreManagerFactory;
  protected final MutableClock timeSource = MutableClock.of(Instant.now(), ZoneOffset.UTC);
  private final RealmContext realmContext = () -> "realmName";

  private TaskFileIOSupplier buildTaskFileIOSupplier(FileIO fileIO) {
    return new TaskFileIOSupplier(
        new FileIOFactory() {
          @Override
          public FileIO loadFileIO(
              @Nonnull CallContext callContext,
              @Nonnull String ioImplClassName,
              @Nonnull Map<String, String> properties,
              @Nonnull TableIdentifier identifier,
              @Nonnull Set<String> tableLocations,
              @Nonnull Set<PolarisStorageActions> storageActions,
              @Nonnull PolarisResolvedPathWrapper resolvedEntityPath) {
            return fileIO;
          }
        });
  }

  private void addTaskLocation(TaskEntity task) {
    Map<String, String> internalPropertiesAsMap = new HashMap<>(task.getInternalPropertiesAsMap());
    internalPropertiesAsMap.put(PolarisTaskConstants.STORAGE_LOCATION, "file:///tmp/");
    ((PolarisBaseEntity) task).setInternalPropertiesAsMap(internalPropertiesAsMap);
  }

  @Test
  void testTaskRecovery() throws IOException {
    // Step 1: Initialize mock table metadata, snapshot, and statistics file to simulate a realistic
    // Iceberg table
    PolarisCallContext polarisCallContext =
        new PolarisCallContext(
            metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get(),
            new PolarisDefaultDiagServiceImpl(),
            new PolarisConfigurationStore() {},
            timeSource);
    try (CallContext callCtx = CallContext.of(realmContext, polarisCallContext)) {
      CallContext.setCurrentContext(callCtx);
      Map<String, AtomicInteger> retryCounter = new HashMap<>();
      FileIO fileIO =
          new InMemoryFileIO() {
            @Override
            public void close() {
              // no-op
            }

            @Override
            public void deleteFile(String location) {
              int attempts =
                  retryCounter
                      .computeIfAbsent(location, k -> new AtomicInteger(0))
                      .incrementAndGet();
              if (attempts == 1) {
                throw new RuntimeException("I'm failing to test retries");
              } else {
                // succeed on the new attempt
                super.deleteFile(location);
              }
            }
          };
      TaskExecutorImpl taskExecutor =
          new TaskExecutorImpl(
              Executors.newSingleThreadExecutor(),
              metaStoreManagerFactory,
              buildTaskFileIOSupplier(fileIO));
      taskExecutor.init();

      TableCleanupTaskHandler tableCleanupTaskHandler =
          new TableCleanupTaskHandler(
              Mockito.mock(),
              metaStoreManagerFactory,
              buildTaskFileIOSupplier(new InMemoryFileIO()));
      TableIdentifier tableIdentifier =
          TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
      long snapshotId = 100L;
      ManifestFile manifestFile =
          TaskTestUtils.manifestFile(
              fileIO, "manifest1.avro", snapshotId, "dataFile1.parquet", "dataFile2.parquet");
      TestSnapshot snapshot =
          TaskTestUtils.newSnapshot(fileIO, "manifestList.avro", 1, snapshotId, 99L, manifestFile);
      String metadataFile = "v1-49494949.metadata.json";
      StatisticsFile statisticsFile =
          TaskTestUtils.writeStatsFile(
              snapshot.snapshotId(),
              snapshot.sequenceNumber(),
              "/metadata/" + UUID.randomUUID() + ".stats",
              fileIO);
      TaskTestUtils.writeTableMetadata(fileIO, metadataFile, List.of(statisticsFile), snapshot);

      // Step 2: Execute the initial cleanup task which generates two child cleanup tasks
      TaskEntity task =
          new TaskEntity.Builder()
              .setName("cleanup_" + tableIdentifier)
              .withTaskType(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER)
              .withData(
                  new IcebergTableLikeEntity.Builder(tableIdentifier, metadataFile)
                      .setName("table1")
                      .setCatalogId(1)
                      .setCreateTimestamp(100)
                      .build())
              .build();
      addTaskLocation(task);
      Assertions.assertThatPredicate(tableCleanupTaskHandler::canHandleTask).accepts(task);
      tableCleanupTaskHandler.handleTask(task, callCtx);

      // Step 3: Verify that the generated child tasks were registered, ATTEMPT_COUNT of 1
      // Batch cleanup task succession needs 2 attempts, the manifest cleanup task succession needs
      // 3 attempts
      EntitiesResult entitiesResult =
          metaStoreManagerFactory
              .getOrCreateMetaStoreManager(realmContext)
              .loadTasks(callCtx.getPolarisCallContext(), "test", 2);
      assertThat(entitiesResult.getEntities()).hasSize(2);
      entitiesResult
          .getEntities()
          .forEach(
              entity -> {
                TaskEntity taskEntity = TaskEntity.of(entity);
                assertThat(taskEntity.getPropertiesAsMap().get(PolarisTaskConstants.ATTEMPT_COUNT))
                    .isEqualTo("1");
              });

      // Step 4: Simulate task timeout and trigger recovery via recoverPendingTasks
      // 4.1 Before timeout: expect no tasks eligible for recovery
      entitiesResult =
          metaStoreManagerFactory
              .getOrCreateMetaStoreManager(realmContext)
              .loadTasks(callCtx.getPolarisCallContext(), "test", 2);
      assertThat(entitiesResult.getEntities()).hasSize(0);
      // 4.2 Advance time and trigger timeout recovery: expect ATTEMPT_COUNT = 2
      timeSource.add(Duration.ofMinutes(10));
      taskExecutor.recoverPendingTasks(timeSource);

      // 4.3 Advance time again and trigger recovery: with ATTEMPT_COUNT = 3
      // Attempts tracker: batch cleanup task (1/2), manifest cleanup task (1/3)
      timeSource.add(Duration.ofMinutes(10));
      entitiesResult =
          metaStoreManagerFactory
              .getOrCreateMetaStoreManager(realmContext)
              .loadTasks(callCtx.getPolarisCallContext(), "test", 2);
      assertThat(entitiesResult.getEntities()).hasSize(2);
      entitiesResult
          .getEntities()
          .forEach(
              entity -> {
                TaskEntity taskEntity = TaskEntity.of(entity);
                assertThat(taskEntity.getPropertiesAsMap().get(PolarisTaskConstants.ATTEMPT_COUNT))
                    .isEqualTo("3");
              });

      // Step 5: Recover tasks again, expect one to succeed and one to remain, ATTEMPT_COUNT = 4
      // Attempts tracker: batch cleanup task (2/2), manifest cleanup task (2/3)
      timeSource.add(Duration.ofMinutes(10));
      taskExecutor.recoverPendingTasks(timeSource);

      // Step 5 Checkpoint: one task succeeded, the other retried and has ATTEMPT_COUNT = 5
      timeSource.add(Duration.ofMinutes(10));
      entitiesResult =
          metaStoreManagerFactory
              .getOrCreateMetaStoreManager(realmContext)
              .loadTasks(callCtx.getPolarisCallContext(), "test", 2);
      assertThat(entitiesResult.getEntities()).hasSize(1);
      entitiesResult
          .getEntities()
          .forEach(
              entity -> {
                TaskEntity taskEntity = TaskEntity.of(entity);
                assertThat(taskEntity.getPropertiesAsMap().get(PolarisTaskConstants.ATTEMPT_COUNT))
                    .isEqualTo("5");
              });

      // Step 6: Final recovery after timeout: all tasks should succeed
      // Attempts tracker: manifest cleanup task (3/3)
      timeSource.add(Duration.ofMinutes(10));
      taskExecutor.recoverPendingTasks(timeSource);

      timeSource.add(Duration.ofMinutes(10));
      entitiesResult =
          metaStoreManagerFactory
              .getOrCreateMetaStoreManager(realmContext)
              .loadTasks(callCtx.getPolarisCallContext(), "test", 2);
      assertThat(entitiesResult.getEntities()).hasSize(0);
    }
  }
}

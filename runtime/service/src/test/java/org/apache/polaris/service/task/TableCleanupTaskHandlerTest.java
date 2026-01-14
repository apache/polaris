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

import static org.apache.polaris.service.task.TaskTestUtils.addTaskLocation;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

@QuarkusTest
class TableCleanupTaskHandlerTest {
  @Inject Clock clock;
  @Inject MetaStoreManagerFactory metaStoreManagerFactory;
  @Inject PolarisMetaStoreManager metaStoreManager;
  @Inject CallContext callContext;
  @InjectMock TaskFileIOSupplier taskFileIOSupplier;

  private final RealmContext realmContext = () -> "realmName";

  private TableCleanupTaskHandler newTableCleanupTaskHandler(FileIO fileIO) {
    Mockito.when(taskFileIOSupplier.apply(Mockito.any(), Mockito.any())).thenReturn(fileIO);
    return new TableCleanupTaskHandler(
        Mockito.mock(), clock, metaStoreManagerFactory, taskFileIOSupplier);
  }

  @BeforeEach
  void setup() {
    QuarkusMock.installMockForType(realmContext, RealmContext.class);
  }

  @Test
  public void testTableCleanup() throws IOException {
    FileIO fileIO = new InMemoryFileIO();
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    TableCleanupTaskHandler handler = newTableCleanupTaskHandler(fileIO);
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

    TaskEntity task =
        new TaskEntity.Builder()
            .setName("cleanup_" + tableIdentifier)
            .withTaskType(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER)
            .withData(
                new IcebergTableLikeEntity.Builder(
                        PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier, metadataFile)
                    .setName("table1")
                    .setCatalogId(1)
                    .setCreateTimestamp(100)
                    .build())
            .build();
    task = addTaskLocation(task);
    Assertions.assertThatPredicate(handler::canHandleTask).accepts(task);

    handler.handleTask(task, callContext);

    assertThat(
            metaStoreManager
                .loadTasks(callContext.getPolarisCallContext(), "test", PageToken.fromLimit(2))
                .getEntities())
        .hasSize(2)
        .satisfiesExactlyInAnyOrder(
            taskEntity ->
                assertThat(taskEntity)
                    .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                    .extracting(TaskEntity::of)
                    .returns(AsyncTaskType.MANIFEST_FILE_CLEANUP, TaskEntity::getTaskType)
                    .returns(
                        ManifestFileCleanupTaskHandler.ManifestCleanupTask.buildFrom(
                            tableIdentifier, manifestFile),
                        entity ->
                            entity.readData(
                                ManifestFileCleanupTaskHandler.ManifestCleanupTask.class)),
            taskEntity ->
                assertThat(taskEntity)
                    .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                    .extracting(TaskEntity::of)
                    .returns(AsyncTaskType.BATCH_FILE_CLEANUP, TaskEntity::getTaskType)
                    .returns(
                        new BatchFileCleanupTaskHandler.BatchFileCleanupTask(
                            tableIdentifier,
                            List.of(snapshot.manifestListLocation(), statisticsFile.path())),
                        entity ->
                            entity.readData(
                                BatchFileCleanupTaskHandler.BatchFileCleanupTask.class)));
  }

  @Test
  public void testTableCleanupHandlesAlreadyDeletedMetadata() throws IOException {
    FileIO fileIO =
        new InMemoryFileIO() {
          @Override
          public void close() {
            // no-op
          }
        };
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    TableCleanupTaskHandler handler = newTableCleanupTaskHandler(fileIO);
    long snapshotId = 100L;
    ManifestFile manifestFile =
        TaskTestUtils.manifestFile(
            fileIO, "manifest1.avro", snapshotId, "dataFile1.parquet", "dataFile2.parquet");
    TestSnapshot snapshot =
        TaskTestUtils.newSnapshot(fileIO, "manifestList.avro", 1, snapshotId, 99L, manifestFile);
    String metadataFile = "v1-49494949.metadata.json";
    TaskTestUtils.writeTableMetadata(fileIO, metadataFile, snapshot);

    IcebergTableLikeEntity icebergTableLikeEntity =
        new IcebergTableLikeEntity.Builder(
                PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier, metadataFile)
            .setName("table1")
            .setCatalogId(1)
            .setCreateTimestamp(100)
            .build();
    TaskEntity task =
        new TaskEntity.Builder()
            .setName("cleanup_" + tableIdentifier)
            .withTaskType(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER)
            .withData(icebergTableLikeEntity)
            .build();
    task = addTaskLocation(task);
    Assertions.assertThatPredicate(handler::canHandleTask).accepts(task);

    // handle the same task twice
    // the first one should successfully delete the metadata
    List<Boolean> results =
        List.of(handler.handleTask(task, callContext), handler.handleTask(task, callContext));
    assertThat(results).containsExactly(true, true);

    // both tasks successfully executed, but only one should queue subtasks
    assertThat(
            metaStoreManager
                .loadTasks(callContext.getPolarisCallContext(), "test", PageToken.fromLimit(5))
                .getEntities())
        .hasSize(2);
  }

  @Test
  public void testTableCleanupDuplicatesTasksIfFileStillExists() throws IOException {
    FileIO fileIO =
        new InMemoryFileIO() {
          @Override
          public void deleteFile(String location) {
            LoggerFactory.getLogger(TableCleanupTaskHandler.class)
                .info(
                    "Not deleting file at location {} to simulate concurrent tasks runs", location);
            // don't do anything
          }

          @Override
          public void close() {
            // no-op
          }
        };
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    TableCleanupTaskHandler handler = newTableCleanupTaskHandler(fileIO);
    long snapshotId = 100L;
    ManifestFile manifestFile =
        TaskTestUtils.manifestFile(
            fileIO, "manifest1.avro", snapshotId, "dataFile1.parquet", "dataFile2.parquet");
    TestSnapshot snapshot =
        TaskTestUtils.newSnapshot(fileIO, "manifestList.avro", 1, snapshotId, 99L, manifestFile);
    String metadataFile = "v1-49494949.metadata.json";
    TaskTestUtils.writeTableMetadata(fileIO, metadataFile, snapshot);

    TaskEntity task =
        new TaskEntity.Builder()
            .setName("cleanup_" + tableIdentifier)
            .withTaskType(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER)
            .withData(
                new IcebergTableLikeEntity.Builder(
                        PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier, metadataFile)
                    .setName("table1")
                    .setCatalogId(1)
                    .setCreateTimestamp(100)
                    .build())
            .build();
    task = addTaskLocation(task);
    Assertions.assertThatPredicate(handler::canHandleTask).accepts(task);

    // handle the same task twice
    // the first one should successfully delete the metadata
    List<Boolean> results =
        List.of(handler.handleTask(task, callContext), handler.handleTask(task, callContext));
    assertThat(results).containsExactly(true, true);

    // both tasks successfully executed, but only one should queue subtasks
    assertThat(
            metaStoreManager
                .loadTasks(callContext.getPolarisCallContext(), "test", PageToken.fromLimit(5))
                .getEntities())
        .hasSize(4)
        .satisfiesExactlyInAnyOrder(
            taskEntity ->
                assertThat(taskEntity)
                    .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                    .extracting(TaskEntity::of)
                    .returns(AsyncTaskType.BATCH_FILE_CLEANUP, TaskEntity::getTaskType)
                    .returns(
                        new BatchFileCleanupTaskHandler.BatchFileCleanupTask(
                            tableIdentifier, List.of(snapshot.manifestListLocation())),
                        entity ->
                            entity.readData(
                                BatchFileCleanupTaskHandler.BatchFileCleanupTask.class)),
            taskEntity ->
                assertThat(taskEntity)
                    .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                    .extracting(TaskEntity::of)
                    .returns(AsyncTaskType.BATCH_FILE_CLEANUP, TaskEntity::getTaskType)
                    .returns(
                        new BatchFileCleanupTaskHandler.BatchFileCleanupTask(
                            tableIdentifier, List.of(snapshot.manifestListLocation())),
                        entity ->
                            entity.readData(
                                BatchFileCleanupTaskHandler.BatchFileCleanupTask.class)),
            taskEntity ->
                assertThat(taskEntity)
                    .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                    .extracting(TaskEntity::of)
                    .returns(AsyncTaskType.MANIFEST_FILE_CLEANUP, TaskEntity::getTaskType)
                    .returns(
                        ManifestFileCleanupTaskHandler.ManifestCleanupTask.buildFrom(
                            tableIdentifier, manifestFile),
                        entity ->
                            entity.readData(
                                ManifestFileCleanupTaskHandler.ManifestCleanupTask.class)),
            taskEntity ->
                assertThat(taskEntity)
                    .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                    .extracting(TaskEntity::of)
                    .returns(AsyncTaskType.MANIFEST_FILE_CLEANUP, TaskEntity::getTaskType)
                    .returns(
                        ManifestFileCleanupTaskHandler.ManifestCleanupTask.buildFrom(
                            tableIdentifier, manifestFile),
                        entity ->
                            entity.readData(
                                ManifestFileCleanupTaskHandler.ManifestCleanupTask.class)));
  }

  @Test
  public void testTableCleanupMultipleSnapshots() throws IOException {
    FileIO fileIO = new InMemoryFileIO();
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    TableCleanupTaskHandler handler = newTableCleanupTaskHandler(fileIO);
    long snapshotId1 = 100L;
    ManifestFile manifestFile1 =
        TaskTestUtils.manifestFile(
            fileIO, "manifest1.avro", snapshotId1, "dataFile1.parquet", "dataFile2.parquet");
    ManifestFile manifestFile2 =
        TaskTestUtils.manifestFile(
            fileIO, "manifest2.avro", snapshotId1, "dataFile3.parquet", "dataFile4.parquet");
    Snapshot snapshot =
        TaskTestUtils.newSnapshot(
            fileIO, "manifestList.avro", 1, snapshotId1, 99L, manifestFile1, manifestFile2);
    ManifestFile manifestFile3 =
        TaskTestUtils.manifestFile(
            fileIO, "manifest3.avro", snapshot.snapshotId() + 1, "dataFile5.parquet");
    Snapshot snapshot2 =
        TaskTestUtils.newSnapshot(
            fileIO,
            "manifestList2.avro",
            snapshot.sequenceNumber() + 1,
            snapshot.snapshotId() + 1,
            snapshot.snapshotId(),
            manifestFile1,
            manifestFile3); // exclude manifest2 from the new snapshot
    String metadataFile = "v1-295495059.metadata.json";
    StatisticsFile statisticsFile1 =
        TaskTestUtils.writeStatsFile(
            snapshot.snapshotId(),
            snapshot.sequenceNumber(),
            "/metadata/" + UUID.randomUUID() + ".stats",
            fileIO);
    StatisticsFile statisticsFile2 =
        TaskTestUtils.writeStatsFile(
            snapshot2.snapshotId(),
            snapshot2.sequenceNumber(),
            "/metadata/" + UUID.randomUUID() + ".stats",
            fileIO);
    TaskTestUtils.writeTableMetadata(
        fileIO, metadataFile, List.of(statisticsFile1, statisticsFile2), snapshot, snapshot2);

    TaskEntity task =
        new TaskEntity.Builder()
            .setName("cleanup_" + tableIdentifier)
            .withTaskType(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER)
            .withData(
                new IcebergTableLikeEntity.Builder(
                        PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier, metadataFile)
                    .setName("table1")
                    .setCatalogId(1)
                    .setCreateTimestamp(100)
                    .build())
            .build();
    task = addTaskLocation(task);
    Assertions.assertThatPredicate(handler::canHandleTask).accepts(task);

    handler.handleTask(task, callContext);

    List<PolarisBaseEntity> entities =
        metaStoreManager
            .loadTasks(callContext.getPolarisCallContext(), "test", PageToken.fromLimit(5))
            .getEntities();

    List<PolarisBaseEntity> manifestCleanupTasks =
        entities.stream()
            .filter(
                entity -> {
                  AsyncTaskType taskType = TaskEntity.of(entity).getTaskType();
                  return taskType == AsyncTaskType.MANIFEST_FILE_CLEANUP;
                })
            .toList();
    List<PolarisBaseEntity> metadataCleanupTasks =
        entities.stream()
            .filter(
                entity -> {
                  AsyncTaskType taskType = TaskEntity.of(entity).getTaskType();
                  return taskType == AsyncTaskType.BATCH_FILE_CLEANUP;
                })
            .toList();

    assertThat(metadataCleanupTasks)
        .hasSize(1)
        .satisfiesExactlyInAnyOrder(
            taskEntity ->
                assertThat(taskEntity)
                    .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                    .extracting(TaskEntity::of)
                    .returns(
                        new BatchFileCleanupTaskHandler.BatchFileCleanupTask(
                            tableIdentifier,
                            List.of(
                                snapshot.manifestListLocation(),
                                snapshot2.manifestListLocation(),
                                statisticsFile1.path(),
                                statisticsFile2.path())),
                        entity ->
                            entity.readData(
                                BatchFileCleanupTaskHandler.BatchFileCleanupTask.class)));

    assertThat(manifestCleanupTasks)
        // all three manifests should be present, even though one is excluded from the latest
        // snapshot
        .hasSize(3)
        .satisfiesExactlyInAnyOrder(
            taskEntity ->
                assertThat(taskEntity)
                    .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                    .extracting(TaskEntity::of)
                    .returns(
                        ManifestFileCleanupTaskHandler.ManifestCleanupTask.buildFrom(
                            tableIdentifier, manifestFile1),
                        entity ->
                            entity.readData(
                                ManifestFileCleanupTaskHandler.ManifestCleanupTask.class)),
            taskEntity ->
                assertThat(taskEntity)
                    .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                    .extracting(TaskEntity::of)
                    .returns(
                        ManifestFileCleanupTaskHandler.ManifestCleanupTask.buildFrom(
                            tableIdentifier, manifestFile2),
                        entity ->
                            entity.readData(
                                ManifestFileCleanupTaskHandler.ManifestCleanupTask.class)),
            taskEntity ->
                assertThat(taskEntity)
                    .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                    .extracting(TaskEntity::of)
                    .returns(
                        ManifestFileCleanupTaskHandler.ManifestCleanupTask.buildFrom(
                            tableIdentifier, manifestFile3),
                        entity ->
                            entity.readData(
                                ManifestFileCleanupTaskHandler.ManifestCleanupTask.class)));
  }

  @Test
  public void testTableCleanupWithTaskPersistenceBatching() throws IOException {
    FileIO fileIO = new InMemoryFileIO();
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    TableCleanupTaskHandler handler = newTableCleanupTaskHandler(fileIO);

    // Create 12 snapshots to generate 12 manifest cleanup tasks
    // This tests that task persistence batching works with default batch size of 100
    List<Snapshot> snapshots = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      ManifestFile manifestFile =
          TaskTestUtils.manifestFile(
              fileIO,
              "manifest" + i + ".avro",
              100L + i,
              "dataFile" + (i * 2) + ".parquet",
              "dataFile" + (i * 2 + 1) + ".parquet");
      Snapshot snapshot =
          TaskTestUtils.newSnapshot(
              fileIO,
              "manifestList" + i + ".avro",
              i + 1,
              100L + i,
              i > 0 ? 99L + i : 99L,
              manifestFile);
      snapshots.add(snapshot);
    }

    String metadataFile = "v1-batch-test.metadata.json";
    TaskTestUtils.writeTableMetadata(fileIO, metadataFile, snapshots.toArray(new Snapshot[0]));

    TaskEntity task =
        new TaskEntity.Builder()
            .setName("cleanup_" + tableIdentifier)
            .withTaskType(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER)
            .withData(
                new IcebergTableLikeEntity.Builder(
                        PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier, metadataFile)
                    .setName("table1")
                    .setCatalogId(1)
                    .setCreateTimestamp(100)
                    .build())
            .build();
    task = addTaskLocation(task);

    handler.handleTask(task, callContext);

    // Verify that all tasks were created
    List<PolarisBaseEntity> entities =
        metaStoreManager
            .loadTasks(callContext.getPolarisCallContext(), "test", PageToken.fromLimit(50))
            .getEntities();

    List<PolarisBaseEntity> manifestCleanupTasks =
        entities.stream()
            .filter(
                entity -> {
                  AsyncTaskType taskType = TaskEntity.of(entity).getTaskType();
                  return taskType == AsyncTaskType.MANIFEST_FILE_CLEANUP;
                })
            .toList();
    List<PolarisBaseEntity> metadataCleanupTasks =
        entities.stream()
            .filter(
                entity -> {
                  AsyncTaskType taskType = TaskEntity.of(entity).getTaskType();
                  return taskType == AsyncTaskType.BATCH_FILE_CLEANUP;
                })
            .toList();

    // Should have 12 manifest cleanup tasks (one per unique manifest)
    assertThat(manifestCleanupTasks).hasSize(12);
    // Should have 2 metadata cleanup tasks (12 manifest lists / batch size of 10 = ceil(1.2) = 2)
    assertThat(metadataCleanupTasks).hasSize(2);
  }

  @Test
  public void testManifestDeduplicationAcrossSnapshots() throws IOException {
    FileIO fileIO = new InMemoryFileIO();
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    TableCleanupTaskHandler handler = newTableCleanupTaskHandler(fileIO);

    // Create shared manifest files that appear in multiple snapshots
    ManifestFile sharedManifest1 =
        TaskTestUtils.manifestFile(
            fileIO, "shared-manifest1.avro", 100L, "dataFile1.parquet", "dataFile2.parquet");
    ManifestFile sharedManifest2 =
        TaskTestUtils.manifestFile(
            fileIO, "shared-manifest2.avro", 100L, "dataFile3.parquet", "dataFile4.parquet");
    ManifestFile uniqueManifest =
        TaskTestUtils.manifestFile(fileIO, "unique-manifest.avro", 101L, "dataFile5.parquet");

    // Create multiple snapshots that share manifests
    Snapshot snapshot1 =
        TaskTestUtils.newSnapshot(
            fileIO, "manifestList1.avro", 1, 100L, 99L, sharedManifest1, sharedManifest2);
    Snapshot snapshot2 =
        TaskTestUtils.newSnapshot(
            fileIO,
            "manifestList2.avro",
            2,
            101L,
            100L,
            sharedManifest1,
            sharedManifest2,
            uniqueManifest);
    Snapshot snapshot3 =
        TaskTestUtils.newSnapshot(
            fileIO, "manifestList3.avro", 3, 102L, 101L, sharedManifest2, uniqueManifest);

    String metadataFile = "v1-dedup-test.metadata.json";
    TaskTestUtils.writeTableMetadata(fileIO, metadataFile, snapshot1, snapshot2, snapshot3);

    TaskEntity task =
        new TaskEntity.Builder()
            .setName("cleanup_" + tableIdentifier)
            .withTaskType(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER)
            .withData(
                new IcebergTableLikeEntity.Builder(
                        PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier, metadataFile)
                    .setName("table1")
                    .setCatalogId(1)
                    .setCreateTimestamp(100)
                    .build())
            .build();
    task = addTaskLocation(task);

    handler.handleTask(task, callContext);

    List<PolarisBaseEntity> entities =
        metaStoreManager
            .loadTasks(callContext.getPolarisCallContext(), "test", PageToken.fromLimit(10))
            .getEntities();

    List<PolarisBaseEntity> manifestCleanupTasks =
        entities.stream()
            .filter(
                entity -> {
                  AsyncTaskType taskType = TaskEntity.of(entity).getTaskType();
                  return taskType == AsyncTaskType.MANIFEST_FILE_CLEANUP;
                })
            .toList();

    // Should have exactly 3 manifest cleanup tasks (deduplicated):
    // sharedManifest1, sharedManifest2, and uniqueManifest
    assertThat(manifestCleanupTasks)
        .hasSize(3)
        .satisfiesExactlyInAnyOrder(
            taskEntity ->
                assertThat(taskEntity)
                    .extracting(TaskEntity::of)
                    .returns(
                        ManifestFileCleanupTaskHandler.ManifestCleanupTask.buildFrom(
                            tableIdentifier, sharedManifest1),
                        entity ->
                            entity.readData(
                                ManifestFileCleanupTaskHandler.ManifestCleanupTask.class)),
            taskEntity ->
                assertThat(taskEntity)
                    .extracting(TaskEntity::of)
                    .returns(
                        ManifestFileCleanupTaskHandler.ManifestCleanupTask.buildFrom(
                            tableIdentifier, sharedManifest2),
                        entity ->
                            entity.readData(
                                ManifestFileCleanupTaskHandler.ManifestCleanupTask.class)),
            taskEntity ->
                assertThat(taskEntity)
                    .extracting(TaskEntity::of)
                    .returns(
                        ManifestFileCleanupTaskHandler.ManifestCleanupTask.buildFrom(
                            tableIdentifier, uniqueManifest),
                        entity ->
                            entity.readData(
                                ManifestFileCleanupTaskHandler.ManifestCleanupTask.class)));
  }

  @Test
  public void testMetadataFileBatchingWithManyFiles() throws IOException {
    FileIO fileIO = new InMemoryFileIO();
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    TableCleanupTaskHandler handler = newTableCleanupTaskHandler(fileIO);

    // Create 12 snapshots to generate 24 metadata files (manifest lists + stats files)
    // With batch size of 10, this should create 3 batches: [10, 10, 4]
    List<Snapshot> snapshots = new ArrayList<>();
    List<String> expectedManifestLists = new ArrayList<>();
    List<StatisticsFile> statsFiles = new ArrayList<>();

    for (int i = 0; i < 12; i++) {
      ManifestFile manifestFile =
          TaskTestUtils.manifestFile(
              fileIO, "manifest" + i + ".avro", 100L + i, "dataFile" + i + ".parquet");
      Snapshot snapshot =
          TaskTestUtils.newSnapshot(
              fileIO,
              "manifestList" + i + ".avro",
              i + 1,
              100L + i,
              i > 0 ? 99L + i : 99L,
              manifestFile);
      snapshots.add(snapshot);
      expectedManifestLists.add(snapshot.manifestListLocation());

      StatisticsFile statsFile =
          TaskTestUtils.writeStatsFile(
              snapshot.snapshotId(),
              snapshot.sequenceNumber(),
              "/metadata/stats-" + UUID.randomUUID() + ".stats",
              fileIO);
      statsFiles.add(statsFile);
    }

    String metadataFile = "v1-many-files.metadata.json";
    TaskTestUtils.writeTableMetadata(
        fileIO, metadataFile, statsFiles, snapshots.toArray(new Snapshot[0]));

    TaskEntity task =
        new TaskEntity.Builder()
            .setName("cleanup_" + tableIdentifier)
            .withTaskType(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER)
            .withData(
                new IcebergTableLikeEntity.Builder(
                        PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier, metadataFile)
                    .setName("table1")
                    .setCatalogId(1)
                    .setCreateTimestamp(100)
                    .build())
            .build();
    task = addTaskLocation(task);

    handler.handleTask(task, callContext);

    List<PolarisBaseEntity> entities =
        metaStoreManager
            .loadTasks(callContext.getPolarisCallContext(), "test", PageToken.fromLimit(50))
            .getEntities();

    List<PolarisBaseEntity> metadataCleanupTasks =
        entities.stream()
            .filter(
                entity -> {
                  AsyncTaskType taskType = TaskEntity.of(entity).getTaskType();
                  return taskType == AsyncTaskType.BATCH_FILE_CLEANUP;
                })
            .toList();

    // With 12 snapshots, we have 24 metadata files (12 manifest lists + 12 stats files)
    // With default batch size of 10, we should have ceil(24/10) = 3 batch cleanup tasks
    assertThat(metadataCleanupTasks).hasSize(3);

    // Verify that all metadata files are included across all batches
    List<String> allFilesInTasks = new ArrayList<>();
    for (PolarisBaseEntity entity : metadataCleanupTasks) {
      BatchFileCleanupTaskHandler.BatchFileCleanupTask batchTask =
          TaskEntity.of(entity).readData(BatchFileCleanupTaskHandler.BatchFileCleanupTask.class);
      allFilesInTasks.addAll(batchTask.batchFiles());
    }

    List<String> expectedAllFiles = new ArrayList<>();
    expectedAllFiles.addAll(expectedManifestLists);
    expectedAllFiles.addAll(statsFiles.stream().map(StatisticsFile::path).toList());

    assertThat(allFilesInTasks).containsExactlyInAnyOrderElementsOf(expectedAllFiles);
  }

  @Test
  public void testTableCleanupMultipleMetadata() throws IOException {
    FileIO fileIO = new InMemoryFileIO();
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    TableCleanupTaskHandler handler = newTableCleanupTaskHandler(fileIO);
    long snapshotId1 = 100L;
    ManifestFile manifestFile1 =
        TaskTestUtils.manifestFile(
            fileIO, "manifest1.avro", snapshotId1, "dataFile1.parquet", "dataFile2.parquet");
    ManifestFile manifestFile2 =
        TaskTestUtils.manifestFile(
            fileIO, "manifest2.avro", snapshotId1, "dataFile3.parquet", "dataFile4.parquet");
    Snapshot snapshot =
        TaskTestUtils.newSnapshot(
            fileIO, "manifestList.avro", 1, snapshotId1, 99L, manifestFile1, manifestFile2);
    StatisticsFile statisticsFile1 =
        TaskTestUtils.writeStatsFile(
            snapshot.snapshotId(),
            snapshot.sequenceNumber(),
            "/metadata/" + UUID.randomUUID() + ".stats",
            fileIO);
    PartitionStatisticsFile partitionStatisticsFile1 =
        TaskTestUtils.writePartitionStatsFile(
            snapshot.snapshotId(),
            "/metadata/" + "partition-stats-" + UUID.randomUUID() + ".parquet",
            fileIO);
    String firstMetadataFile = "v1-295495059.metadata.json";
    TableMetadata firstMetadata =
        TaskTestUtils.writeTableMetadata(
            fileIO,
            firstMetadataFile,
            List.of(statisticsFile1),
            List.of(partitionStatisticsFile1),
            snapshot);
    assertThat(TaskUtils.exists(firstMetadataFile, fileIO)).isTrue();

    ManifestFile manifestFile3 =
        TaskTestUtils.manifestFile(
            fileIO, "manifest3.avro", snapshot.snapshotId() + 1, "dataFile5.parquet");
    Snapshot snapshot2 =
        TaskTestUtils.newSnapshot(
            fileIO,
            "manifestList2.avro",
            snapshot.sequenceNumber() + 1,
            snapshot.snapshotId() + 1,
            snapshot.snapshotId(),
            manifestFile1,
            manifestFile3); // exclude manifest2 from the new snapshot
    StatisticsFile statisticsFile2 =
        TaskTestUtils.writeStatsFile(
            snapshot2.snapshotId(),
            snapshot2.sequenceNumber(),
            "/metadata/" + UUID.randomUUID() + ".stats",
            fileIO);
    PartitionStatisticsFile partitionStatisticsFile2 =
        TaskTestUtils.writePartitionStatsFile(
            snapshot2.snapshotId(),
            "/metadata/" + "partition-stats-" + UUID.randomUUID() + ".parquet",
            fileIO);
    String secondMetadataFile = "v1-295495060.metadata.json";
    TaskTestUtils.writeTableMetadata(
        fileIO,
        secondMetadataFile,
        firstMetadata,
        firstMetadataFile,
        List.of(statisticsFile2),
        List.of(partitionStatisticsFile2),
        snapshot2);
    assertThat(TaskUtils.exists(firstMetadataFile, fileIO)).isTrue();
    assertThat(TaskUtils.exists(secondMetadataFile, fileIO)).isTrue();

    TaskEntity task =
        new TaskEntity.Builder()
            .setName("cleanup_" + tableIdentifier)
            .withTaskType(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER)
            .withData(
                new IcebergTableLikeEntity.Builder(
                        PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier, secondMetadataFile)
                    .setName("table1")
                    .setCatalogId(1)
                    .setCreateTimestamp(100)
                    .build())
            .build();
    task = addTaskLocation(task);

    Assertions.assertThatPredicate(handler::canHandleTask).accepts(task);

    handler.handleTask(task, callContext);

    List<PolarisBaseEntity> entities =
        metaStoreManager
            .loadTasks(callContext.getPolarisCallContext(), "test", PageToken.fromLimit(6))
            .getEntities();

    List<PolarisBaseEntity> manifestCleanupTasks =
        entities.stream()
            .filter(
                entity -> {
                  AsyncTaskType taskType = TaskEntity.of(entity).getTaskType();
                  return taskType == AsyncTaskType.MANIFEST_FILE_CLEANUP;
                })
            .toList();
    List<PolarisBaseEntity> metadataCleanupTasks =
        entities.stream()
            .filter(
                entity -> {
                  AsyncTaskType taskType = TaskEntity.of(entity).getTaskType();
                  return taskType == AsyncTaskType.BATCH_FILE_CLEANUP;
                })
            .toList();

    assertThat(metadataCleanupTasks)
        .hasSize(1)
        .satisfiesExactlyInAnyOrder(
            taskEntity ->
                assertThat(taskEntity)
                    .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                    .extracting(TaskEntity::of)
                    .returns(
                        new BatchFileCleanupTaskHandler.BatchFileCleanupTask(
                            tableIdentifier,
                            List.of(
                                firstMetadataFile,
                                snapshot.manifestListLocation(),
                                snapshot2.manifestListLocation(),
                                statisticsFile1.path(),
                                statisticsFile2.path(),
                                partitionStatisticsFile1.path(),
                                partitionStatisticsFile2.path())),
                        entity ->
                            entity.readData(
                                BatchFileCleanupTaskHandler.BatchFileCleanupTask.class)));

    assertThat(manifestCleanupTasks)
        .hasSize(3)
        .satisfiesExactlyInAnyOrder(
            taskEntity ->
                assertThat(taskEntity)
                    .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                    .extracting(TaskEntity::of)
                    .returns(
                        ManifestFileCleanupTaskHandler.ManifestCleanupTask.buildFrom(
                            tableIdentifier, manifestFile1),
                        entity ->
                            entity.readData(
                                ManifestFileCleanupTaskHandler.ManifestCleanupTask.class)),
            taskEntity ->
                assertThat(taskEntity)
                    .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                    .extracting(TaskEntity::of)
                    .returns(
                        ManifestFileCleanupTaskHandler.ManifestCleanupTask.buildFrom(
                            tableIdentifier, manifestFile2),
                        entity ->
                            entity.readData(
                                ManifestFileCleanupTaskHandler.ManifestCleanupTask.class)),
            taskEntity ->
                assertThat(taskEntity)
                    .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                    .extracting(TaskEntity::of)
                    .returns(
                        ManifestFileCleanupTaskHandler.ManifestCleanupTask.buildFrom(
                            tableIdentifier, manifestFile3),
                        entity ->
                            entity.readData(
                                ManifestFileCleanupTaskHandler.ManifestCleanupTask.class)));
  }
}

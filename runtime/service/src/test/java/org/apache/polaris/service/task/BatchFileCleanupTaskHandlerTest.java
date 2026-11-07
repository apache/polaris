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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatPredicate;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.TaskEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

@QuarkusTest
public class BatchFileCleanupTaskHandlerTest {
  @Inject CallContext callContext;
  @InjectMock TaskFileIOSupplier taskFileIOSupplier;

  private final RealmContext realmContext = () -> "realmName";
  private PolarisCallContext polarisCallContext;
  private ExecutorService executor;

  @BeforeEach
  public void beforeEach() {
    QuarkusMock.installMockForType(realmContext, RealmContext.class);
    polarisCallContext = callContext.getPolarisCallContext();
    executor = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  public void afterEach() {
    executor.close();
  }

  private BatchFileCleanupTaskHandler newBatchFileCleanupTaskHandler(FileIO fileIO) {
    Mockito.when(taskFileIOSupplier.apply(Mockito.any(), Mockito.any())).thenReturn(fileIO);
    return new BatchFileCleanupTaskHandler(taskFileIOSupplier, executor, Duration.ofMinutes(5));
  }

  @Test
  public void testMetadataFileCleanup() throws IOException {
    FileIO fileIO =
        new InMemoryFileIO() {
          @Override
          public void close() {
            // no-op
          }
        };
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    BatchFileCleanupTaskHandler handler = newBatchFileCleanupTaskHandler(fileIO);

    long snapshotId1 = 100L;
    ManifestFile manifestFile1 =
        TaskTestUtils.manifestFile(
            fileIO,
            TASK_LOCATION + "manifest1.avro",
            snapshotId1,
            TASK_LOCATION + "dataFile1.parquet",
            TASK_LOCATION + "dataFile2.parquet");
    ManifestFile manifestFile2 =
        TaskTestUtils.manifestFile(
            fileIO,
            TASK_LOCATION + "manifest2.avro",
            snapshotId1,
            TASK_LOCATION + "dataFile3.parquet",
            TASK_LOCATION + "dataFile4.parquet");
    Snapshot snapshot =
        TaskTestUtils.newSnapshot(
            fileIO,
            TASK_LOCATION + "manifestList.avro",
            1,
            snapshotId1,
            99L,
            manifestFile1,
            manifestFile2);
    StatisticsFile statisticsFile1 =
        TaskTestUtils.writeStatsFile(
            snapshot.snapshotId(),
            snapshot.sequenceNumber(),
            TASK_LOCATION + "metadata/" + UUID.randomUUID() + ".stats",
            fileIO);
    PartitionStatisticsFile partitionStatisticsFile1 =
        TaskTestUtils.writePartitionStatsFile(
            snapshot.snapshotId(),
            TASK_LOCATION + "metadata/" + "partition-stats-" + UUID.randomUUID() + ".parquet",
            fileIO);
    String firstMetadataFile = TASK_LOCATION + "v1-295495059.metadata.json";
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
            fileIO,
            TASK_LOCATION + "manifest3.avro",
            snapshot.snapshotId() + 1,
            TASK_LOCATION + "dataFile5.parquet");
    Snapshot snapshot2 =
        TaskTestUtils.newSnapshot(
            fileIO,
            TASK_LOCATION + "manifestList2.avro",
            snapshot.sequenceNumber() + 1,
            snapshot.snapshotId() + 1,
            snapshot.snapshotId(),
            manifestFile1,
            manifestFile3); // exclude manifest2 from the new snapshot
    StatisticsFile statisticsFile2 =
        TaskTestUtils.writeStatsFile(
            snapshot2.snapshotId(),
            snapshot2.sequenceNumber(),
            TASK_LOCATION + "metadata/" + UUID.randomUUID() + ".stats",
            fileIO);
    PartitionStatisticsFile partitionStatisticsFile2 =
        TaskTestUtils.writePartitionStatsFile(
            snapshot2.snapshotId(),
            TASK_LOCATION + "metadata/" + "partition-stats-" + UUID.randomUUID() + ".parquet",
            fileIO);
    String secondMetadataFile = TASK_LOCATION + "v1-295495060.metadata.json";
    TableMetadata secondMetadata =
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

    List<String> cleanupFiles =
        Stream.of(
                secondMetadata.previousFiles().stream().map(TableMetadata.MetadataLogEntry::file),
                secondMetadata.statisticsFiles().stream().map(StatisticsFile::path),
                secondMetadata.partitionStatisticsFiles().stream()
                    .map(PartitionStatisticsFile::path))
            .flatMap(s -> s)
            .filter(file -> TaskUtils.exists(file, fileIO))
            .toList();

    TaskEntity task =
        new TaskEntity.Builder()
            .withTaskType(AsyncTaskType.BATCH_FILE_CLEANUP)
            .withData(
                new BatchFileCleanupTaskHandler.BatchFileCleanupTask(
                    tableIdentifier,
                    cleanupFiles,
                    BatchFileCleanupTaskHandler.BatchFileType.TABLE_METADATA))
            .setName(UUID.randomUUID().toString())
            .build();

    task = addTaskLocation(task);
    TaskEntity finalTask = task;
    assertThatPredicate(handler::canHandleTask).accepts(task);
    assertThatCode(() -> handler.handleTask(finalTask, polarisCallContext))
        .doesNotThrowAnyException();

    for (String cleanupFile : cleanupFiles) {
      assertThatPredicate((String file) -> TaskUtils.exists(file, fileIO)).rejects(cleanupFile);
    }
  }

  @Test
  public void testMetadataFileCleanupIfFileNotExist() throws IOException {
    FileIO fileIO = new InMemoryFileIO();
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    BatchFileCleanupTaskHandler handler = newBatchFileCleanupTaskHandler(fileIO);
    long snapshotId = 100L;
    ManifestFile manifestFile =
        TaskTestUtils.manifestFile(
            fileIO,
            TASK_LOCATION + "manifest1.avro",
            snapshotId,
            TASK_LOCATION + "dataFile1.parquet",
            TASK_LOCATION + "dataFile2.parquet");
    TestSnapshot snapshot =
        TaskTestUtils.newSnapshot(
            fileIO, TASK_LOCATION + "manifestList.avro", 1, snapshotId, 99L, manifestFile);
    String metadataFile = TASK_LOCATION + "v1-49494949.metadata.json";
    StatisticsFile statisticsFile =
        TaskTestUtils.writeStatsFile(
            snapshot.snapshotId(),
            snapshot.sequenceNumber(),
            TASK_LOCATION + "metadata/" + UUID.randomUUID() + ".stats",
            fileIO);
    TaskTestUtils.writeTableMetadata(fileIO, metadataFile, List.of(statisticsFile), snapshot);

    fileIO.deleteFile(statisticsFile.path());
    assertThat(TaskUtils.exists(statisticsFile.path(), fileIO)).isFalse();

    TaskEntity task =
        new TaskEntity.Builder()
            .withTaskType(AsyncTaskType.BATCH_FILE_CLEANUP)
            .withData(
                new BatchFileCleanupTaskHandler.BatchFileCleanupTask(
                    tableIdentifier,
                    List.of(statisticsFile.path()),
                    BatchFileCleanupTaskHandler.BatchFileType.TABLE_METADATA))
            .setName(UUID.randomUUID().toString())
            .build();

    TaskEntity taskWithLocation = addTaskLocation(task);
    assertThatPredicate(handler::canHandleTask).accepts(taskWithLocation);
    assertThatNoException()
        .isThrownBy(() -> handler.handleTask(taskWithLocation, polarisCallContext));
  }

  @Test
  public void testExistenceCheckIsPerformedOncePerFile() {
    String presentFile1 = TASK_LOCATION + "present1";
    String presentFile2 = TASK_LOCATION + "present2";
    String missingFile = TASK_LOCATION + "missing";

    InputFile presentInput = Mockito.mock(InputFile.class);
    Mockito.when(presentInput.exists()).thenReturn(true);
    InputFile missingInput = Mockito.mock(InputFile.class);
    Mockito.when(missingInput.exists()).thenReturn(false);

    FileIO fileIO = Mockito.mock(FileIO.class);
    Mockito.when(fileIO.newInputFile(presentFile1)).thenReturn(presentInput);
    Mockito.when(fileIO.newInputFile(presentFile2)).thenReturn(presentInput);
    Mockito.when(fileIO.newInputFile(missingFile)).thenReturn(missingInput);

    BatchFileCleanupTaskHandler handler = newBatchFileCleanupTaskHandler(fileIO);
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    TaskEntity task =
        new TaskEntity.Builder()
            .withTaskType(AsyncTaskType.BATCH_FILE_CLEANUP)
            .withData(
                new BatchFileCleanupTaskHandler.BatchFileCleanupTask(
                    tableIdentifier,
                    List.of(presentFile1, presentFile2, missingFile),
                    BatchFileCleanupTaskHandler.BatchFileType.TABLE_METADATA))
            .setName(UUID.randomUUID().toString())
            .build();
    TaskEntity taskWithLocation = addTaskLocation(task);

    assertThatCode(() -> handler.handleTask(taskWithLocation, polarisCallContext))
        .doesNotThrowAnyException();

    // Each file must have been checked exactly once by the pre-delete scan.
    Mockito.verify(fileIO, Mockito.times(1)).newInputFile(presentFile1);
    Mockito.verify(fileIO, Mockito.times(1)).newInputFile(presentFile2);
    Mockito.verify(fileIO, Mockito.times(1)).newInputFile(missingFile);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  public void testCleanupWithRetries(int maxRetries) throws IOException {
    AtomicInteger batchRetryCounter = new AtomicInteger(0);
    FileIO fileIO =
        new InMemoryFileIO() {
          @Override
          public void close() {
            // no-op
          }
        };
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    Mockito.when(taskFileIOSupplier.apply(Mockito.any(), Mockito.any())).thenReturn(fileIO);
    BatchFileCleanupTaskHandler handler =
        new BatchFileCleanupTaskHandler(taskFileIOSupplier, executor, Duration.ofMinutes(5)) {
          @Override
          public CompletableFuture<Void> tryDelete(
              TableIdentifier tableId,
              FileIO fileIO,
              Iterable<String> files,
              String type,
              Boolean isConcurrent,
              Throwable e,
              int attempt) {
            if (attempt <= maxRetries) {
              batchRetryCounter.incrementAndGet();
              return tryDelete(tableId, fileIO, files, type, isConcurrent, e, attempt + 1);
            } else {
              return super.tryDelete(tableId, fileIO, files, type, isConcurrent, e, attempt);
            }
          }
        };
    long snapshotId = 100L;
    ManifestFile manifestFile =
        TaskTestUtils.manifestFile(
            fileIO,
            TASK_LOCATION + "manifest1.avro",
            snapshotId,
            TASK_LOCATION + "dataFile1.parquet",
            TASK_LOCATION + "dataFile2.parquet");
    TestSnapshot snapshot =
        TaskTestUtils.newSnapshot(
            fileIO, TASK_LOCATION + "manifestList.avro", 1, snapshotId, 99L, manifestFile);
    String metadataFile = TASK_LOCATION + "v1-49494949.metadata.json";
    StatisticsFile statisticsFile =
        TaskTestUtils.writeStatsFile(
            snapshot.snapshotId(),
            snapshot.sequenceNumber(),
            TASK_LOCATION + "metadata/" + UUID.randomUUID() + ".stats",
            fileIO);
    TaskTestUtils.writeTableMetadata(fileIO, metadataFile, List.of(statisticsFile), snapshot);
    assertThat(TaskUtils.exists(statisticsFile.path(), fileIO)).isTrue();

    TaskEntity task =
        new TaskEntity.Builder()
            .withTaskType(AsyncTaskType.BATCH_FILE_CLEANUP)
            .withData(
                new BatchFileCleanupTaskHandler.BatchFileCleanupTask(
                    tableIdentifier,
                    List.of(statisticsFile.path()),
                    BatchFileCleanupTaskHandler.BatchFileType.TABLE_METADATA))
            .setName(UUID.randomUUID().toString())
            .build();

    CompletableFuture<Void> future =
        CompletableFuture.runAsync(
            () -> {
              var newTask = addTaskLocation(task);
              assertThatPredicate(handler::canHandleTask).accepts(newTask);
              handler.handleTask(
                  newTask, polarisCallContext); // this will schedule the batch deletion
            });

    // Wait for all async tasks to finish
    future.join();

    // Ensure that retries happened as expected
    assertThat(batchRetryCounter.get()).isEqualTo(maxRetries);

    // Check if the file was successfully deleted after retries
    assertThat(TaskUtils.exists(statisticsFile.path(), fileIO)).isFalse();
  }

  @Test
  public void testFileOutsideTableLocationIsNotDeleted() {
    InMemoryFileIO fileIO =
        new InMemoryFileIO() {
          @Override
          public void close() {
            // no-op
          }
        };
    String insideFile = TASK_LOCATION + "metadata/v1.metadata.json";
    String outsideFile = "file:///other-table/metadata/v1.metadata.json";
    fileIO.addFile(insideFile, new byte[] {1});
    fileIO.addFile(outsideFile, new byte[] {1});
    BatchFileCleanupTaskHandler handler = newBatchFileCleanupTaskHandler(fileIO);
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    TaskEntity task =
        addTaskLocation(
            new TaskEntity.Builder()
                .withTaskType(AsyncTaskType.BATCH_FILE_CLEANUP)
                .withData(
                    new BatchFileCleanupTaskHandler.BatchFileCleanupTask(
                        tableIdentifier,
                        List.of(insideFile, outsideFile),
                        BatchFileCleanupTaskHandler.BatchFileType.TABLE_METADATA))
                .setName(UUID.randomUUID().toString())
                .build());

    handler.handleTask(task, polarisCallContext);

    assertThat(TaskUtils.exists(insideFile, fileIO)).isFalse();
    assertThat(TaskUtils.exists(outsideFile, fileIO)).isTrue();
  }

  @Test
  public void testFileUnderAnyRecordedTableLocationIsDeleted() {
    InMemoryFileIO fileIO =
        new InMemoryFileIO() {
          @Override
          public void close() {
            // no-op
          }
        };
    String insideFile = TASK_LOCATION + "metadata/v1.metadata.json";
    String customDataLocation = "file:///custom-data/";
    String customLocationFile = customDataLocation + "data1.parquet";
    String outsideFile = "file:///other-table/data2.parquet";
    fileIO.addFile(insideFile, new byte[] {1});
    fileIO.addFile(customLocationFile, new byte[] {1});
    fileIO.addFile(outsideFile, new byte[] {1});
    BatchFileCleanupTaskHandler handler = newBatchFileCleanupTaskHandler(fileIO);
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    TaskEntity task =
        new TaskEntity.Builder()
            .withTaskType(AsyncTaskType.BATCH_FILE_CLEANUP)
            .withData(
                new BatchFileCleanupTaskHandler.BatchFileCleanupTask(
                    tableIdentifier,
                    List.of(insideFile, customLocationFile, outsideFile),
                    BatchFileCleanupTaskHandler.BatchFileType.TABLE_METADATA))
            .setName(UUID.randomUUID().toString())
            .build();
    Map<String, String> properties = new HashMap<>(task.getInternalPropertiesAsMap());
    properties.put(
        PolarisTaskConstants.STORAGE_LOCATIONS,
        TaskUtils.encodeStorageLocations(Set.of(TASK_LOCATION, customDataLocation)));
    TaskEntity taskWithLocations =
        TaskEntity.of(
            new PolarisBaseEntity.Builder(task).internalPropertiesAsMap(properties).build());

    handler.handleTask(taskWithLocations, polarisCallContext);

    assertThat(TaskUtils.exists(insideFile, fileIO)).isFalse();
    assertThat(TaskUtils.exists(customLocationFile, fileIO)).isFalse();
    assertThat(TaskUtils.exists(outsideFile, fileIO)).isTrue();
  }
}

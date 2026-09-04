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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatPredicate;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
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
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.TaskEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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
    return new BatchFileCleanupTaskHandler(taskFileIOSupplier, executor);
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
    String presentFile1 = "s3://bucket/present1";
    String presentFile2 = "s3://bucket/present2";
    String missingFile = "s3://bucket/missing";

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

    assertThatCode(() -> handler.handleTask(task, polarisCallContext)).doesNotThrowAnyException();

    // Each file must have been checked exactly once by the pre-delete scan.
    Mockito.verify(fileIO, Mockito.times(1)).newInputFile(presentFile1);
    Mockito.verify(fileIO, Mockito.times(1)).newInputFile(presentFile2);
    Mockito.verify(fileIO, Mockito.times(1)).newInputFile(missingFile);
  }

  @ParameterizedTest
  @CsvSource({
    "false, 0",
    "false, 1",
    "false, 2",
    "false, 3",
    "true, 0",
    "true, 1",
    "true, 2",
    "true, 3"
  })
  public void testCleanupWithRetries(boolean bulk, int failures) {
    String file = "s3://bucket/metadata.json";
    FileIO fileIO = bulk ? Mockito.mock(SupportsBulkOperations.class) : Mockito.mock(FileIO.class);
    InputFile inputFile = Mockito.mock(InputFile.class);
    Mockito.when(inputFile.exists()).thenReturn(true);
    Mockito.when(fileIO.newInputFile(file)).thenReturn(inputFile);
    AtomicInteger deleteCalls = new AtomicInteger();
    RuntimeException failure =
        bulk ? new BulkDeletionFailureException(1) : new IllegalStateException("delete failed");
    var deletion =
        Mockito.doAnswer(
            invocation -> {
              if (deleteCalls.incrementAndGet() <= failures) {
                throw failure;
              }
              return null;
            });
    if (bulk) {
      deletion.when((SupportsBulkOperations) fileIO).deleteFiles(List.of(file));
    } else {
      deletion.when(fileIO).deleteFile(file);
    }

    BatchFileCleanupTaskHandler handler = newBatchFileCleanupTaskHandler(fileIO);
    TaskEntity task =
        new TaskEntity.Builder()
            .withTaskType(AsyncTaskType.BATCH_FILE_CLEANUP)
            .withData(
                new BatchFileCleanupTaskHandler.BatchFileCleanupTask(
                    TableIdentifier.of("db", "table"),
                    List.of(file),
                    BatchFileCleanupTaskHandler.BatchFileType.TABLE_METADATA))
            .setName(UUID.randomUUID().toString())
            .build();

    if (failures >= FileCleanupTaskHandler.MAX_ATTEMPTS) {
      assertThatThrownBy(() -> handler.handleTask(task, polarisCallContext)).hasRootCause(failure);
    } else {
      assertThatNoException().isThrownBy(() -> handler.handleTask(task, polarisCallContext));
    }
    assertThat(deleteCalls.get())
        .isEqualTo(Math.min(failures + 1, FileCleanupTaskHandler.MAX_ATTEMPTS));
    if (bulk) {
      Mockito.verify(fileIO, Mockito.never()).deleteFile(Mockito.anyString());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testPartialDeletionRetriesWithAlreadyDeletedFiles(boolean concurrent)
      throws IOException {
    AtomicInteger deleteCalls = new AtomicInteger();
    String retryFile = UUID.randomUUID() + ".metadata.json";
    FileIO fileIO =
        new InMemoryFileIO() {
          @Override
          public void deleteFile(String path) {
            if (path.equals(retryFile)
                && deleteCalls.incrementAndGet() < FileCleanupTaskHandler.MAX_ATTEMPTS) {
              throw new IllegalStateException("transient delete failure");
            }
            super.deleteFile(path);
          }
        };
    List<String> files = List.of(UUID.randomUUID() + ".metadata.json", retryFile);
    for (String file : files) {
      fileIO.newOutputFile(file).create().close();
    }
    BatchFileCleanupTaskHandler handler = newBatchFileCleanupTaskHandler(fileIO);

    assertThatNoException()
        .isThrownBy(
            () ->
                handler
                    .tryDelete(
                        TableIdentifier.of("db", "table"),
                        fileIO,
                        files,
                        "table_metadata",
                        concurrent,
                        null,
                        1)
                    .join());

    assertThat(deleteCalls.get()).isEqualTo(FileCleanupTaskHandler.MAX_ATTEMPTS);
    for (String file : files) {
      assertThat(TaskUtils.exists(file, fileIO)).isFalse();
    }
  }
}

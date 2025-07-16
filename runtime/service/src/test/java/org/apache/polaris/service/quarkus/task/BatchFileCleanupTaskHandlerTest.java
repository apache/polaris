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

import static org.apache.polaris.service.quarkus.task.TaskTestUtils.addTaskLocation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatPredicate;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.TestFileIOFactory;
import org.apache.polaris.service.task.BatchFileCleanupTaskHandler;
import org.apache.polaris.service.task.TaskFileIOSupplier;
import org.apache.polaris.service.task.TaskUtils;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class BatchFileCleanupTaskHandlerTest {
  @Inject MetaStoreManagerFactory metaStoreManagerFactory;
  private final RealmContext realmContext = () -> "realmName";

  private TaskFileIOSupplier buildTaskFileIOSupplier(FileIO fileIO) {
    return new TaskFileIOSupplier(new TestFileIOFactory(fileIO));
  }

  @Test
  public void testMetadataFileCleanup() throws IOException {
    PolarisCallContext polarisCallContext =
        new PolarisCallContext(
            realmContext,
            metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get(),
            new PolarisDefaultDiagServiceImpl());
    FileIO fileIO =
        new InMemoryFileIO() {
          @Override
          public void close() {
            // no-op
          }
        };
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    BatchFileCleanupTaskHandler handler =
        new BatchFileCleanupTaskHandler(
            buildTaskFileIOSupplier(fileIO), Executors.newSingleThreadExecutor());

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
                new BatchFileCleanupTaskHandler.BatchFileCleanupTask(tableIdentifier, cleanupFiles))
            .setName(UUID.randomUUID().toString())
            .build();

    task = addTaskLocation(task);
    assertThatPredicate(handler::canHandleTask).accepts(task);
    assertThat(handler.handleTask(task, polarisCallContext)).isTrue();

    for (String cleanupFile : cleanupFiles) {
      assertThatPredicate((String file) -> TaskUtils.exists(file, fileIO)).rejects(cleanupFile);
    }
  }

  @Test
  public void testMetadataFileCleanupIfFileNotExist() throws IOException {
    PolarisCallContext polarisCallContext =
        new PolarisCallContext(
            realmContext,
            metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get(),
            new PolarisDefaultDiagServiceImpl());
    CallContext.setCurrentContext(polarisCallContext);
    FileIO fileIO = new InMemoryFileIO();
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    BatchFileCleanupTaskHandler handler =
        new BatchFileCleanupTaskHandler(
            buildTaskFileIOSupplier(fileIO), Executors.newSingleThreadExecutor());
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
                    tableIdentifier, List.of(statisticsFile.path())))
            .setName(UUID.randomUUID().toString())
            .build();

    task = addTaskLocation(task);
    assertThatPredicate(handler::canHandleTask).accepts(task);
    assertThat(handler.handleTask(task, polarisCallContext)).isTrue();
  }

  @Test
  public void testCleanupWithRetries() throws IOException {
    PolarisCallContext polarisCallContext =
        new PolarisCallContext(
            realmContext,
            metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get(),
            new PolarisDefaultDiagServiceImpl());
    CallContext.setCurrentContext(polarisCallContext);
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
                retryCounter.computeIfAbsent(location, k -> new AtomicInteger(0)).incrementAndGet();
            if (attempts < 3) {
              throw new RuntimeException("Simulating failure to test retries");
            } else {
              super.deleteFile(location);
            }
          }
        };
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    BatchFileCleanupTaskHandler handler =
        new BatchFileCleanupTaskHandler(
            buildTaskFileIOSupplier(fileIO), Executors.newSingleThreadExecutor());
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
    assertThat(TaskUtils.exists(statisticsFile.path(), fileIO)).isTrue();

    TaskEntity task =
        new TaskEntity.Builder()
            .withTaskType(AsyncTaskType.BATCH_FILE_CLEANUP)
            .withData(
                new BatchFileCleanupTaskHandler.BatchFileCleanupTask(
                    tableIdentifier, List.of(statisticsFile.path())))
            .setName(UUID.randomUUID().toString())
            .build();

    CompletableFuture<Void> future =
        CompletableFuture.runAsync(
            () -> {
              CallContext.setCurrentContext(polarisCallContext);
              var newTask = addTaskLocation(task);
              assertThatPredicate(handler::canHandleTask).accepts(newTask);
              handler.handleTask(
                  newTask, polarisCallContext); // this will schedule the batch deletion
            });

    // Wait for all async tasks to finish
    future.join();

    // Check if the file was successfully deleted after retries
    assertThat(TaskUtils.exists(statisticsFile.path(), fileIO)).isFalse();

    // Ensure that retries happened as expected
    assertThat(retryCounter.containsKey(statisticsFile.path())).isTrue();
    assertThat(retryCounter.get(statisticsFile.path()).get()).isEqualTo(3);
  }
}

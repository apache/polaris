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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.commons.codec.binary.Base64;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmId;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.service.task.ManifestFileCleanupTaskHandler;
import org.apache.polaris.service.task.TaskUtils;
import org.junit.jupiter.api.Test;

@QuarkusTest
class ManifestFileCleanupTaskHandlerTest {

  @Inject PolarisDiagnostics diagnostics;

  private final RealmId realmId = RealmId.newRealmId("realmName");

  @Test
  public void testCleanupFileNotExists() throws IOException {
    FileIO fileIO = new InMemoryFileIO();
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    ManifestFileCleanupTaskHandler handler =
        new ManifestFileCleanupTaskHandler(
            (task, rc) -> fileIO, Executors.newSingleThreadExecutor(), diagnostics);
    ManifestFile manifestFile =
        TaskTestUtils.manifestFile(
            fileIO, "manifest1.avro", 1L, "dataFile1.parquet", "dataFile2.parquet");
    fileIO.deleteFile(manifestFile.path());
    TaskEntity task =
        new TaskEntity.Builder()
            .withTaskType(diagnostics, AsyncTaskType.MANIFEST_FILE_CLEANUP)
            .withData(
                diagnostics,
                new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                    tableIdentifier, Base64.encodeBase64String(ManifestFiles.encode(manifestFile))))
            .setName(UUID.randomUUID().toString())
            .build();
    assertThat(handler.canHandleTask(task)).isTrue();
    assertThat(handler.handleTask(task, realmId)).isTrue();
  }

  @Test
  public void testCleanupFileManifestExistsDataFilesDontExist() throws IOException {
    FileIO fileIO = new InMemoryFileIO();
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    ManifestFileCleanupTaskHandler handler =
        new ManifestFileCleanupTaskHandler(
            (task, rc) -> fileIO, Executors.newSingleThreadExecutor(), diagnostics);
    ManifestFile manifestFile =
        TaskTestUtils.manifestFile(
            fileIO, "manifest1.avro", 100L, "dataFile1.parquet", "dataFile2.parquet");
    TaskEntity task =
        new TaskEntity.Builder()
            .withTaskType(diagnostics, AsyncTaskType.MANIFEST_FILE_CLEANUP)
            .withData(
                diagnostics,
                new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                    tableIdentifier, Base64.encodeBase64String(ManifestFiles.encode(manifestFile))))
            .setName(UUID.randomUUID().toString())
            .build();
    assertThat(handler.canHandleTask(task)).isTrue();
    assertThat(handler.handleTask(task, realmId)).isTrue();
  }

  @Test
  public void testCleanupFiles() throws IOException {
    FileIO fileIO =
        new InMemoryFileIO() {
          @Override
          public void close() {
            // no-op
          }
        };
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    ManifestFileCleanupTaskHandler handler =
        new ManifestFileCleanupTaskHandler(
            (task, rc) -> fileIO, Executors.newSingleThreadExecutor(), diagnostics);
    String dataFile1Path = "dataFile1.parquet";
    OutputFile dataFile1 = fileIO.newOutputFile(dataFile1Path);
    PositionOutputStream out1 = dataFile1.createOrOverwrite();
    out1.write("the data".getBytes(UTF_8));
    out1.close();
    String dataFile2Path = "dataFile2.parquet";
    OutputFile dataFile2 = fileIO.newOutputFile(dataFile2Path);
    PositionOutputStream out2 = dataFile2.createOrOverwrite();
    out2.write("the data".getBytes(UTF_8));
    out2.close();
    ManifestFile manifestFile =
        TaskTestUtils.manifestFile(fileIO, "manifest1.avro", 100L, dataFile1Path, dataFile2Path);
    TaskEntity task =
        new TaskEntity.Builder()
            .withTaskType(diagnostics, AsyncTaskType.MANIFEST_FILE_CLEANUP)
            .withData(
                diagnostics,
                new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                    tableIdentifier, Base64.encodeBase64String(ManifestFiles.encode(manifestFile))))
            .setName(UUID.randomUUID().toString())
            .build();
    assertThat(handler.canHandleTask(task)).isTrue();
    assertThat(handler.handleTask(task, realmId)).isTrue();
    assertThat(TaskUtils.exists(dataFile1Path, fileIO)).isFalse();
    assertThat(TaskUtils.exists(dataFile2Path, fileIO)).isFalse();
  }

  @Test
  public void testCleanupFilesWithRetries() throws IOException {
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
              throw new RuntimeException("I'm failing to test retries");
            } else {
              // succeed on the third attempt
              super.deleteFile(location);
            }
          }
        };

    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    ManifestFileCleanupTaskHandler handler =
        new ManifestFileCleanupTaskHandler(
            (task, rc) -> fileIO, Executors.newSingleThreadExecutor(), diagnostics);
    String dataFile1Path = "dataFile1.parquet";
    OutputFile dataFile1 = fileIO.newOutputFile(dataFile1Path);
    PositionOutputStream out1 = dataFile1.createOrOverwrite();
    out1.write("the data".getBytes(UTF_8));
    out1.close();
    String dataFile2Path = "dataFile2.parquet";
    OutputFile dataFile2 = fileIO.newOutputFile(dataFile2Path);
    PositionOutputStream out2 = dataFile2.createOrOverwrite();
    out2.write("the data".getBytes(UTF_8));
    out2.close();
    ManifestFile manifestFile =
        TaskTestUtils.manifestFile(fileIO, "manifest1.avro", 100L, dataFile1Path, dataFile2Path);
    TaskEntity task =
        new TaskEntity.Builder()
            .withTaskType(diagnostics, AsyncTaskType.MANIFEST_FILE_CLEANUP)
            .withData(
                diagnostics,
                new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                    tableIdentifier, Base64.encodeBase64String(ManifestFiles.encode(manifestFile))))
            .setName(UUID.randomUUID().toString())
            .build();
    assertThat(handler.canHandleTask(task)).isTrue();
    assertThat(handler.handleTask(task, realmId)).isTrue();
    assertThat(TaskUtils.exists(dataFile1Path, fileIO)).isFalse();
    assertThat(TaskUtils.exists(dataFile2Path, fileIO)).isFalse();
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
    ManifestFileCleanupTaskHandler handler =
        new ManifestFileCleanupTaskHandler(
            (task, rc) -> fileIO, Executors.newSingleThreadExecutor(), diagnostics);

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
    String firstMetadataFile = "v1-295495059.metadata.json";
    TableMetadata firstMetadata =
        TaskTestUtils.writeTableMetadata(
            fileIO, firstMetadataFile, List.of(statisticsFile1), snapshot);
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
    String secondMetadataFile = "v1-295495060.metadata.json";
    TableMetadata secondMetadata =
        TaskTestUtils.writeTableMetadata(
            fileIO,
            secondMetadataFile,
            firstMetadata,
            firstMetadataFile,
            List.of(statisticsFile2),
            snapshot2);
    assertThat(TaskUtils.exists(firstMetadataFile, fileIO)).isTrue();
    assertThat(TaskUtils.exists(secondMetadataFile, fileIO)).isTrue();

    List<String> cleanupFiles =
        Stream.concat(
                secondMetadata.previousFiles().stream()
                    .map(metadataLogEntry -> metadataLogEntry.file())
                    .filter(file -> TaskUtils.exists(file, fileIO)),
                secondMetadata.statisticsFiles().stream()
                    .map(statisticsFile -> statisticsFile.path())
                    .filter(file -> TaskUtils.exists(file, fileIO)))
            .toList();

    TaskEntity task =
        new TaskEntity.Builder()
            .withTaskType(diagnostics, AsyncTaskType.METADATA_FILE_BATCH_CLEANUP)
            .withData(
                diagnostics,
                new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                    tableIdentifier, cleanupFiles))
            .setName(UUID.randomUUID().toString())
            .build();

    assertThat(handler.canHandleTask(task)).isTrue();
    assertThat(handler.handleTask(task, realmId)).isTrue();

    assertThat(TaskUtils.exists(firstMetadataFile, fileIO)).isFalse();
    assertThat(TaskUtils.exists(statisticsFile1.path(), fileIO)).isFalse();
    assertThat(TaskUtils.exists(statisticsFile2.path(), fileIO)).isFalse();
  }

  @Test
  public void testMetadataFileCleanupIfFileNotExist() throws IOException {
    FileIO fileIO = new InMemoryFileIO();
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
    ManifestFileCleanupTaskHandler handler =
        new ManifestFileCleanupTaskHandler(
            (task, rc) -> fileIO, Executors.newSingleThreadExecutor(), diagnostics);
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
            .withTaskType(diagnostics, AsyncTaskType.METADATA_FILE_BATCH_CLEANUP)
            .withData(
                diagnostics,
                new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                    tableIdentifier, List.of(statisticsFile.path())))
            .setName(UUID.randomUUID().toString())
            .build();
    assertThat(handler.canHandleTask(task)).isTrue();
    assertThat(handler.handleTask(task, realmId)).isTrue();
  }

  @Test
  public void testCleanupWithRetries() throws IOException {
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
    ManifestFileCleanupTaskHandler handler =
        new ManifestFileCleanupTaskHandler(
            (task, rc) -> fileIO, Executors.newSingleThreadExecutor(), diagnostics);
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
            .withTaskType(diagnostics, AsyncTaskType.METADATA_FILE_BATCH_CLEANUP)
            .withData(
                diagnostics,
                new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                    tableIdentifier, List.of(statisticsFile.path())))
            .setName(UUID.randomUUID().toString())
            .build();

    try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
      CompletableFuture<Void> future;
      future =
          CompletableFuture.runAsync(
              () -> {
                assertThat(handler.canHandleTask(task)).isTrue();
                handler.handleTask(task, realmId); // this will schedule the batch deletion
              },
              executor);
      // Wait for all async tasks to finish
      future.join();
    }

    // Check if the file was successfully deleted after retries
    assertThat(TaskUtils.exists(statisticsFile.path(), fileIO)).isFalse();

    // Ensure that retries happened as expected
    assertThat(retryCounter.containsKey(statisticsFile.path())).isTrue();
    assertThat(retryCounter.get(statisticsFile.path()).get()).isEqualTo(3);
  }
}

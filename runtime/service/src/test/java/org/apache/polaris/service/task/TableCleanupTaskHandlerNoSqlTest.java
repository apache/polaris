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
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionStatisticsFile;
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
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * NoSQL indexes an entity name as an {@link IndexKey}, which rejects keys longer than 500 bytes.
 */
@QuarkusTest
@TestProfile(TableCleanupTaskHandlerNoSqlTest.NoSqlInMemory.class)
class TableCleanupTaskHandlerNoSqlTest {
  @Inject Clock clock;
  @Inject MetaStoreManagerFactory metaStoreManagerFactory;
  @Inject PolarisMetaStoreManager metaStoreManager;
  @Inject CallContext callContext;
  @InjectMock TaskFileIOSupplier taskFileIOSupplier;

  private final RealmContext realmContext = () -> "realmName";

  public static class NoSqlInMemory implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.persistence.type", "nosql", "polaris.persistence.nosql.backend", "InMemory");
    }
  }

  @BeforeEach
  void setup() {
    QuarkusMock.installMockForType(realmContext, RealmContext.class);
    metaStoreManagerFactory.bootstrapRealms(
        List.of(realmContext.getRealmIdentifier()), RootCredentialsSet.EMPTY);
  }

  @Test
  public void testLongUtf8PathsCreateTasksWithoutExceedingIndexKey() throws IOException {
    FileIO fileIO = new InMemoryFileIO();
    Mockito.when(taskFileIOSupplier.apply(Mockito.any(), Mockito.any())).thenReturn(fileIO);
    TableCleanupTaskHandler handler =
        new TableCleanupTaskHandler(
            Mockito.mock(), clock, metaStoreManagerFactory, taskFileIOSupplier);

    // Multi-byte so a char-length check would miss the IndexKey byte limit.
    String longSegment = "é".repeat(400);
    String manifestPath = "s3://bucket/" + longSegment + "/manifest.avro";
    String secondManifestPath = "s3://bucket/" + longSegment + "/manifest-2.avro";
    String manifestListPath = "s3://bucket/" + longSegment + "/manifest-list.avro";
    String secondManifestListPath = "s3://bucket/" + longSegment + "/manifest-list-2.avro";
    String statsPath = "s3://bucket/" + longSegment + "/stats.puffin";
    String partitionStatsPath = "s3://bucket/" + longSegment + "/partition-stats.parquet";
    String previousMetadataPath = "s3://bucket/" + longSegment + "/v1.metadata.json";
    TableIdentifier tableIdentifier =
        TableIdentifier.of(Namespace.of("db1", "schema1"), "longpath");

    ManifestFile manifestFile =
        TaskTestUtils.manifestFile(fileIO, manifestPath, 100L, "dataFile1.parquet");
    TestSnapshot firstSnapshot =
        TaskTestUtils.newSnapshot(fileIO, manifestListPath, 1, 100L, 99L, manifestFile);
    StatisticsFile firstStats =
        TaskTestUtils.writeStatsFile(
            firstSnapshot.snapshotId(), firstSnapshot.sequenceNumber(), statsPath, fileIO);
    PartitionStatisticsFile firstPartitionStats =
        TaskTestUtils.writePartitionStatsFile(
            firstSnapshot.snapshotId(), partitionStatsPath, fileIO);
    TableMetadata firstMetadata =
        TaskTestUtils.writeTableMetadata(
            fileIO,
            previousMetadataPath,
            List.of(firstStats),
            List.of(firstPartitionStats),
            firstSnapshot);

    ManifestFile secondManifest =
        TaskTestUtils.manifestFile(fileIO, secondManifestPath, 101L, "dataFile2.parquet");
    TestSnapshot secondSnapshot =
        TaskTestUtils.newSnapshot(
            fileIO, secondManifestListPath, 2, 101L, 100L, manifestFile, secondManifest);
    String metadataFile = "v2-long-path.metadata.json";
    TaskTestUtils.writeTableMetadata(
        fileIO,
        metadataFile,
        firstMetadata,
        previousMetadataPath,
        List.of(firstStats),
        List.of(firstPartitionStats),
        secondSnapshot);

    TaskEntity task =
        new TaskEntity.Builder()
            .setName("cleanup_" + tableIdentifier)
            .setId(42L)
            .withTaskType(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER)
            .withData(
                new IcebergTableLikeEntity.Builder(
                        PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier, metadataFile)
                    .setName("longpath")
                    .setCatalogId(1)
                    .setCreateTimestamp(100)
                    .build())
            .build();
    task = addTaskLocation(task);

    handler.handleTask(task, callContext);

    List<PolarisBaseEntity> created =
        metaStoreManager
            .loadTasks(callContext.getPolarisCallContext(), "test", PageToken.fromLimit(20))
            .getEntities();
    assertThat(created).isNotEmpty();
    assertThat(created)
        .allSatisfy(
            entity -> {
              byte[] nameBytes = entity.getName().getBytes(StandardCharsets.UTF_8);
              assertThat(nameBytes).hasSizeLessThanOrEqualTo(IndexKey.MAX_LENGTH);
              assertThat(entity.getName()).doesNotContain(longSegment);
              IndexKey.key(entity.getName());
            });

    assertThat(created)
        .filteredOn(
            entity -> TaskEntity.of(entity).getTaskType() == AsyncTaskType.MANIFEST_FILE_CLEANUP)
        .map(
            entity ->
                TaskEntity.of(entity)
                    .readData(ManifestFileCleanupTaskHandler.ManifestCleanupTask.class))
        .contains(
            ManifestFileCleanupTaskHandler.ManifestCleanupTask.buildFrom(
                tableIdentifier, manifestFile),
            ManifestFileCleanupTaskHandler.ManifestCleanupTask.buildFrom(
                tableIdentifier, secondManifest));

    assertThat(created)
        .filteredOn(
            entity -> TaskEntity.of(entity).getTaskType() == AsyncTaskType.BATCH_FILE_CLEANUP)
        .singleElement()
        .satisfies(
            entity ->
                assertThat(
                        TaskEntity.of(entity)
                            .readData(BatchFileCleanupTaskHandler.BatchFileCleanupTask.class)
                            .batchFiles())
                    .contains(
                        previousMetadataPath,
                        manifestListPath,
                        secondManifestListPath,
                        statsPath,
                        partitionStatsPath));
  }
}

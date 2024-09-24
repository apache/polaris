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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Base64;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.TableLikeEntity;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

class TableCleanupTaskHandlerTest {
  private InMemoryPolarisMetaStoreManagerFactory metaStoreManagerFactory;
  private RealmContext realmContext;

  @BeforeEach
  void setUp() {
    metaStoreManagerFactory = new InMemoryPolarisMetaStoreManagerFactory();
    realmContext = () -> "realmName";
  }

  @Test
  public void testTableCleanup() throws IOException {
    PolarisCallContext polarisCallContext =
        new PolarisCallContext(
            metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get(),
            new PolarisDefaultDiagServiceImpl());
    try (CallContext callCtx = CallContext.of(realmContext, polarisCallContext)) {
      CallContext.setCurrentContext(callCtx);
      FileIO fileIO = createMockFileIO(new InMemoryFileIO());
      TableIdentifier tableIdentifier =
          TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
      TableCleanupTaskHandler handler =
          new TableCleanupTaskHandler(Mockito.mock(), metaStoreManagerFactory, (task) -> fileIO);
      long snapshotId = 100L;
      ManifestFile manifestFile =
          TaskTestUtils.manifestFile(
              fileIO, "manifest1.avro", snapshotId, "dataFile1.parquet", "dataFile2.parquet");
      TestSnapshot snapshot =
          TaskTestUtils.newSnapshot(fileIO, "manifestList.avro", 1, snapshotId, 99L, manifestFile);
      String metadataFile = "v1-49494949.metadata.json";
      StatisticsFile statisticsFile = writeStatsFile(snapshot.snapshotId(), snapshot.sequenceNumber(), "/metadata/" + UUID.randomUUID() + ".stats", fileIO);
      TaskTestUtils.writeTableMetadata(fileIO, metadataFile, List.of(statisticsFile), snapshot);

      TaskEntity task =
          new TaskEntity.Builder()
              .setName("cleanup_" + tableIdentifier.toString())
              .withTaskType(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER)
              .withData(
                  new TableLikeEntity.Builder(tableIdentifier, metadataFile)
                      .setName("table1")
                      .setCatalogId(1)
                      .setCreateTimestamp(100)
                      .build())
              .build();
      Assertions.assertThatPredicate(handler::canHandleTask).accepts(task);

      PolarisBaseEntity baseEntity = task.readData(PolarisBaseEntity.class);
      TableLikeEntity tableEntity = TableLikeEntity.of(baseEntity);
      TableMetadata tableMetadata =
          TableMetadataParser.read(fileIO, tableEntity.getMetadataLocation());
      Set<String> metadataLocations = metadataLocations(tableMetadata);
      Set<String> statsLocation = statsLocations(tableMetadata);
      assertThat(metadataLocations).hasSize(1);
      assertThat(statsLocation).hasSize(1);

      CallContext.setCurrentContext(CallContext.of(realmContext, polarisCallContext));
      handler.handleTask(task);

      assertThat(
              metaStoreManagerFactory
                  .getOrCreateMetaStoreManager(realmContext)
                  .loadTasks(polarisCallContext, "test", 1)
                  .getEntities())
          .hasSize(1)
          .satisfiesExactly(
              taskEntity ->
                  assertThat(taskEntity)
                      .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                      .extracting(entity -> TaskEntity.of(entity))
                      .returns(AsyncTaskType.FILE_CLEANUP, TaskEntity::getTaskType)
                      .returns(
                          new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                              tableIdentifier,
                              Base64.encodeBase64String(ManifestFiles.encode(manifestFile))),
                          entity ->
                              entity.readData(
                                  ManifestFileCleanupTaskHandler.ManifestCleanupTask.class)));

      ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
      Mockito.verify(fileIO, Mockito.times(metadataLocations.size() + statsLocation.size()))
          .deleteFile(argumentCaptor.capture());

      List<String> deletedPaths = argumentCaptor.getAllValues();
      assertThat(deletedPaths)
          .as("should contain all created metadata locations")
          .containsAll(metadataLocations);
      assertThat(deletedPaths)
              .as("should contain all created stats locations")
              .containsAll(statsLocation);
    }
  }

  @Test
  public void testTableCleanupHandlesAlreadyDeletedMetadata() throws IOException {
    PolarisCallContext polarisCallContext =
        new PolarisCallContext(
            metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get(),
            new PolarisDefaultDiagServiceImpl());
    try (CallContext callCtx = CallContext.of(realmContext, polarisCallContext)) {
      CallContext.setCurrentContext(callCtx);
      FileIO fileIO =
          new InMemoryFileIO() {
            @Override
            public void close() {
              // no-op
            }
          };
      TableIdentifier tableIdentifier =
          TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
      TableCleanupTaskHandler handler =
          new TableCleanupTaskHandler(Mockito.mock(), metaStoreManagerFactory, (task) -> fileIO);
      long snapshotId = 100L;
      ManifestFile manifestFile =
          TaskTestUtils.manifestFile(
              fileIO, "manifest1.avro", snapshotId, "dataFile1.parquet", "dataFile2.parquet");
      TestSnapshot snapshot =
          TaskTestUtils.newSnapshot(fileIO, "manifestList.avro", 1, snapshotId, 99L, manifestFile);
      String metadataFile = "v1-49494949.metadata.json";
      TaskTestUtils.writeTableMetadata(fileIO, metadataFile, snapshot);

      TableLikeEntity tableLikeEntity =
          new TableLikeEntity.Builder(tableIdentifier, metadataFile)
              .setName("table1")
              .setCatalogId(1)
              .setCreateTimestamp(100)
              .build();
      TaskEntity task =
          new TaskEntity.Builder()
              .setName("cleanup_" + tableIdentifier.toString())
              .withTaskType(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER)
              .withData(tableLikeEntity)
              .build();
      Assertions.assertThatPredicate(handler::canHandleTask).accepts(task);

      CallContext.setCurrentContext(CallContext.of(realmContext, polarisCallContext));

      // handle the same task twice
      // the first one should successfully delete the metadata
      List<Boolean> results = List.of(handler.handleTask(task), handler.handleTask(task));
      assertThat(results).containsExactly(true, true);

      // both tasks successfully executed, but only one should queue subtasks
      assertThat(
              metaStoreManagerFactory
                  .getOrCreateMetaStoreManager(realmContext)
                  .loadTasks(polarisCallContext, "test", 5)
                  .getEntities())
          .hasSize(1);
    }
  }

  @Test
  public void testTableCleanupDuplicatesTasksIfFileStillExists() throws IOException {
    PolarisCallContext polarisCallContext =
        new PolarisCallContext(
            metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get(),
            new PolarisDefaultDiagServiceImpl());
    try (CallContext callCtx = CallContext.of(realmContext, polarisCallContext)) {
      CallContext.setCurrentContext(callCtx);
      FileIO fileIO =
          new InMemoryFileIO() {
            @Override
            public void deleteFile(String location) {
              LoggerFactory.getLogger(TableCleanupTaskHandler.class)
                  .info(
                      "Not deleting file at location {} to simulate concurrent tasks runs",
                      location);
              // don't do anything
            }

            @Override
            public void close() {
              // no-op
            }
          };
      TableIdentifier tableIdentifier =
          TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
      TableCleanupTaskHandler handler =
          new TableCleanupTaskHandler(Mockito.mock(), metaStoreManagerFactory, (task) -> fileIO);
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
              .setName("cleanup_" + tableIdentifier.toString())
              .withTaskType(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER)
              .withData(
                  new TableLikeEntity.Builder(tableIdentifier, metadataFile)
                      .setName("table1")
                      .setCatalogId(1)
                      .setCreateTimestamp(100)
                      .build())
              .build();
      Assertions.assertThatPredicate(handler::canHandleTask).accepts(task);

      CallContext.setCurrentContext(CallContext.of(realmContext, polarisCallContext));

      // handle the same task twice
      // the first one should successfully delete the metadata
      List<Boolean> results = List.of(handler.handleTask(task), handler.handleTask(task));
      assertThat(results).containsExactly(true, true);

      // both tasks successfully executed, but only one should queue subtasks
      assertThat(
              metaStoreManagerFactory
                  .getOrCreateMetaStoreManager(realmContext)
                  .loadTasks(polarisCallContext, "test", 5)
                  .getEntities())
          .hasSize(2)
          .satisfiesExactly(
              taskEntity ->
                  assertThat(taskEntity)
                      .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                      .extracting(entity -> TaskEntity.of(entity))
                      .returns(AsyncTaskType.FILE_CLEANUP, TaskEntity::getTaskType)
                      .returns(
                          new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                              tableIdentifier,
                              Base64.encodeBase64String(ManifestFiles.encode(manifestFile))),
                          entity ->
                              entity.readData(
                                  ManifestFileCleanupTaskHandler.ManifestCleanupTask.class)),
              taskEntity ->
                  assertThat(taskEntity)
                      .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                      .extracting(entity -> TaskEntity.of(entity))
                      .returns(AsyncTaskType.FILE_CLEANUP, TaskEntity::getTaskType)
                      .returns(
                          new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                              tableIdentifier,
                              Base64.encodeBase64String(ManifestFiles.encode(manifestFile))),
                          entity ->
                              entity.readData(
                                  ManifestFileCleanupTaskHandler.ManifestCleanupTask.class)));
    }
  }

  @Test
  public void testTableCleanupMultipleSnapshots() throws IOException {
    PolarisCallContext polarisCallContext =
        new PolarisCallContext(
            metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get(),
            new PolarisDefaultDiagServiceImpl());
    try (CallContext callCtx = CallContext.of(realmContext, polarisCallContext)) {
      CallContext.setCurrentContext(callCtx);
      FileIO fileIO = createMockFileIO(new InMemoryFileIO());
      TableIdentifier tableIdentifier =
          TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
      TableCleanupTaskHandler handler =
          new TableCleanupTaskHandler(Mockito.mock(), metaStoreManagerFactory, (task) -> fileIO);
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
      StatisticsFile statisticsFile1 = writeStatsFile(snapshot.snapshotId(), snapshot.sequenceNumber(), "/metadata/" + UUID.randomUUID() + ".stats", fileIO);
      StatisticsFile statisticsFile2 = writeStatsFile(snapshot2.snapshotId(), snapshot2.sequenceNumber(), "/metadata/" + UUID.randomUUID() + ".stats", fileIO);
      TaskTestUtils.writeTableMetadata(fileIO, metadataFile, List.of(statisticsFile1, statisticsFile2), snapshot, snapshot2);

      TaskEntity task =
          new TaskEntity.Builder()
              .withTaskType(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER)
              .withData(
                  new TableLikeEntity.Builder(tableIdentifier, metadataFile)
                      .setName("table1")
                      .setCatalogId(1)
                      .setCreateTimestamp(100)
                      .build())
              .build();
      Assertions.assertThatPredicate(handler::canHandleTask).accepts(task);

      CallContext.setCurrentContext(CallContext.of(realmContext, polarisCallContext));

      PolarisBaseEntity baseEntity = task.readData(PolarisBaseEntity.class);
      TableLikeEntity tableEntity = TableLikeEntity.of(baseEntity);
      TableMetadata tableMetadata =
          TableMetadataParser.read(fileIO, tableEntity.getMetadataLocation());
      Set<String> metadataLocations = metadataLocations(tableMetadata);
      Set<String> statsLocations = statsLocations(tableMetadata);
      assertThat(metadataLocations).hasSize(1);
      assertThat(statsLocations).hasSize(2);

      handler.handleTask(task);

      assertThat(
              metaStoreManagerFactory
                  .getOrCreateMetaStoreManager(realmContext)
                  .loadTasks(polarisCallContext, "test", 5)
                  .getEntities())
          // all three manifests should be present, even though one is excluded from the latest
          // snapshot
          .hasSize(3)
          .satisfiesExactlyInAnyOrder(
              taskEntity ->
                  assertThat(taskEntity)
                      .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                      .extracting(entity -> TaskEntity.of(entity))
                      .returns(
                          new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                              tableIdentifier,
                              Base64.encodeBase64String(ManifestFiles.encode(manifestFile1))),
                          entity ->
                              entity.readData(
                                  ManifestFileCleanupTaskHandler.ManifestCleanupTask.class)),
              taskEntity ->
                  assertThat(taskEntity)
                      .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                      .extracting(entity -> TaskEntity.of(entity))
                      .returns(
                          new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                              tableIdentifier,
                              Base64.encodeBase64String(ManifestFiles.encode(manifestFile2))),
                          entity ->
                              entity.readData(
                                  ManifestFileCleanupTaskHandler.ManifestCleanupTask.class)),
              taskEntity ->
                  assertThat(taskEntity)
                      .returns(PolarisEntityType.TASK.getCode(), PolarisBaseEntity::getTypeCode)
                      .extracting(entity -> TaskEntity.of(entity))
                      .returns(
                          new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                              tableIdentifier,
                              Base64.encodeBase64String(ManifestFiles.encode(manifestFile3))),
                          entity ->
                              entity.readData(
                                  ManifestFileCleanupTaskHandler.ManifestCleanupTask.class)));

      ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
      Mockito.verify(fileIO, Mockito.times(metadataLocations.size() + statsLocations.size()))
          .deleteFile(argumentCaptor.capture());

      List<String> deletedPaths = argumentCaptor.getAllValues();
      assertThat(deletedPaths)
          .as("should contain all created metadata locations")
          .containsAll(metadataLocations);
      assertThat(deletedPaths)
        .as("should contain all created stats locations")
              .containsAll(statsLocations);
    }
  }

  private FileIO createMockFileIO(InMemoryFileIO wrapped) {
    InMemoryFileIO mockIO = Mockito.mock(InMemoryFileIO.class);

    Mockito.when(mockIO.newInputFile(Mockito.anyString()))
        .thenAnswer(invocation -> wrapped.newInputFile((String) invocation.getArgument(0)));
    Mockito.when(mockIO.newInputFile(Mockito.anyString(), Mockito.anyLong()))
        .thenAnswer(
            invocation ->
                wrapped.newInputFile(invocation.getArgument(0), invocation.getArgument(1)));
    Mockito.when(mockIO.newInputFile(Mockito.any(ManifestFile.class)))
        .thenAnswer(invocation -> wrapped.newInputFile((ManifestFile) invocation.getArgument(0)));
    Mockito.when(mockIO.newInputFile(Mockito.any(DataFile.class)))
        .thenAnswer(invocation -> wrapped.newInputFile((DataFile) invocation.getArgument(0)));
    Mockito.when(mockIO.newInputFile(Mockito.any(DeleteFile.class)))
        .thenAnswer(invocation -> wrapped.newInputFile((DeleteFile) invocation.getArgument(0)));
    Mockito.when(mockIO.newOutputFile(Mockito.anyString()))
        .thenAnswer(invocation -> wrapped.newOutputFile(invocation.getArgument(0)));

    return mockIO;
  }

  private Set<String> metadataLocations(TableMetadata tableMetadata) {
    Set<String> metadataLocations =
        tableMetadata.previousFiles().stream()
            .map(TableMetadata.MetadataLogEntry::file)
            .collect(Collectors.toSet());
    metadataLocations.add(tableMetadata.metadataFileLocation());
    return metadataLocations;
  }

  private Set<String> statsLocations(TableMetadata tableMetadata) {
    return tableMetadata.statisticsFiles().stream()
        .map(StatisticsFile::path)
        .collect(Collectors.toSet());
  }

  private StatisticsFile writeStatsFile(
      long snapshotId, long snapshotSequenceNumber, String statsLocation, FileIO fileIO)
      throws IOException {
    try (PuffinWriter puffinWriter = Puffin.write(fileIO.newOutputFile(statsLocation)).build()) {
      puffinWriter.add(
          new Blob(
              "some-blob-type",
              List.of(1),
              snapshotId,
              snapshotSequenceNumber,
              ByteBuffer.wrap("blob content".getBytes(StandardCharsets.UTF_8))));
      puffinWriter.finish();

      return new GenericStatisticsFile(
          snapshotId,
          statsLocation,
          puffinWriter.fileSize(),
          puffinWriter.footerSize(),
          puffinWriter.writtenBlobsMetadata().stream().map(GenericBlobMetadata::from).toList());
    }
  }
}

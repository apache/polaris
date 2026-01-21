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

import static org.apache.polaris.core.config.FeatureConfiguration.TABLE_METADATA_CLEANUP_BATCH_SIZE;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table cleanup handler resolves the latest {@link TableMetadata} file for a dropped table and
 * schedules a deletion task for <i>each</i> Snapshot found in the {@link TableMetadata}. Manifest
 * cleanup tasks are scheduled in a batch so tasks should be stored atomically.
 */
public class TableCleanupTaskHandler implements TaskHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableCleanupTaskHandler.class);
  private final TaskExecutor taskExecutor;
  private final Clock clock;
  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final TaskFileIOSupplier fileIOSupplier;
  private static final String TASK_PERSISTENCE_BATCH_SIZE_CONFIG_KEY =
      "TABLE_CLEANUP_TASK_PERSISTENCE_BATCH_SIZE";

  public TableCleanupTaskHandler(
      TaskExecutor taskExecutor,
      Clock clock,
      MetaStoreManagerFactory metaStoreManagerFactory,
      TaskFileIOSupplier fileIOSupplier) {
    this.taskExecutor = taskExecutor;
    this.clock = clock;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.fileIOSupplier = fileIOSupplier;
  }

  @Override
  public boolean canHandleTask(TaskEntity task) {
    return task.getTaskType() == AsyncTaskType.ENTITY_CLEANUP_SCHEDULER
        && tryGetTableEntity(task).isPresent();
  }

  private Optional<IcebergTableLikeEntity> tryGetTableEntity(TaskEntity task) {
    return Optional.ofNullable(task.readData(PolarisBaseEntity.class))
        .filter(entity -> entity.getType().equals(PolarisEntityType.TABLE_LIKE))
        .map(IcebergTableLikeEntity::of);
  }

  @Override
  public boolean handleTask(TaskEntity cleanupTask, CallContext callContext) {
    IcebergTableLikeEntity tableEntity = tryGetTableEntity(cleanupTask).orElseThrow();
    LOGGER
        .atInfo()
        .addKeyValue("tableIdentifier", tableEntity.getTableIdentifier())
        .addKeyValue("metadataLocation", tableEntity.getMetadataLocation())
        .log("Handling table metadata cleanup task");

    // It's likely the cleanupTask has already been completed, but wasn't dropped successfully.
    // Log a
    // warning and move on
    try (FileIO fileIO = fileIOSupplier.apply(cleanupTask, tableEntity.getTableIdentifier())) {
      if (!TaskUtils.exists(tableEntity.getMetadataLocation(), fileIO)) {
        LOGGER
            .atWarn()
            .addKeyValue("tableIdentifier", tableEntity.getTableIdentifier())
            .addKeyValue("metadataLocation", tableEntity.getMetadataLocation())
            .log("Table metadata cleanup scheduled, but metadata file does not exist");
        return true;
      }

      TableMetadata tableMetadata =
          TableMetadataParser.read(fileIO, tableEntity.getMetadataLocation());

      PolarisMetaStoreManager metaStoreManager =
          metaStoreManagerFactory.getOrCreateMetaStoreManager(callContext.getRealmContext());
      PolarisCallContext polarisCallContext = callContext.getPolarisCallContext();

      int taskPersistenceBatchSize =
          callContext.getRealmConfig().getConfig(TASK_PERSISTENCE_BATCH_SIZE_CONFIG_KEY, 100);

      Stream<TaskEntity> manifestCleanupTasks =
          getManifestTaskStream(
              cleanupTask,
              tableMetadata,
              fileIO,
              tableEntity,
              metaStoreManager,
              polarisCallContext);

      Stream<TaskEntity> metadataFileCleanupTasks =
          getMetadataTaskStream(
              cleanupTask, tableMetadata, tableEntity, metaStoreManager, callContext);

      // Process tasks in batches to avoid holding all tasks in memory
      Stream<TaskEntity> allTasks = Stream.concat(manifestCleanupTasks, metadataFileCleanupTasks);
      int totalTasksCreated =
          processTasks(
              allTasks,
              taskPersistenceBatchSize,
              metaStoreManager,
              polarisCallContext,
              tableEntity);

      if (totalTasksCreated > 0) {
        LOGGER
            .atInfo()
            .addKeyValue("tableIdentifier", tableEntity.getTableIdentifier())
            .addKeyValue("metadataLocation", tableEntity.getMetadataLocation())
            .addKeyValue("taskCount", totalTasksCreated)
            .log(
                "Successfully queued tasks to delete manifests, previous metadata, and statistics files - deleting table metadata file");

        fileIO.deleteFile(tableEntity.getMetadataLocation());

        return true;
      }
    }
    return false;
  }

  private int processTasks(
      Stream<TaskEntity> taskStream,
      int batchSize,
      PolarisMetaStoreManager metaStoreManager,
      PolarisCallContext polarisCallContext,
      IcebergTableLikeEntity tableEntity) {
    int totalCount = 0;
    Iterator<TaskEntity> iterator = taskStream.iterator();
    List<TaskEntity> batch = new ArrayList<>(batchSize);

    while (iterator.hasNext()) {
      batch.add(iterator.next());
      if (batch.size() >= batchSize) {
        createAndRegisterTasks(batch, metaStoreManager, polarisCallContext, tableEntity);
        totalCount += batch.size();
        batch.clear();
      }
    }

    // Create remaining tasks
    if (!batch.isEmpty()) {
      createAndRegisterTasks(batch, metaStoreManager, polarisCallContext, tableEntity);
      totalCount += batch.size();
    }

    return totalCount;
  }

  private void createAndRegisterTasks(
      List<TaskEntity> batch,
      PolarisMetaStoreManager metaStoreManager,
      PolarisCallContext polarisCallContext,
      IcebergTableLikeEntity tableEntity) {
    List<PolarisBaseEntity> createdTasks =
        metaStoreManager.createEntitiesIfNotExist(polarisCallContext, null, batch).getEntities();
    if (createdTasks != null) {
      for (PolarisBaseEntity createdTask : createdTasks) {
        taskExecutor.addTaskHandlerContext(createdTask.getId(), polarisCallContext);
      }
      LOGGER
          .atDebug()
          .addKeyValue("tableIdentifier", tableEntity.getTableIdentifier())
          .addKeyValue("batchSize", batch.size())
          .log("Created and registered batch of cleanup tasks");
    }
  }

  private Stream<TaskEntity> getManifestTaskStream(
      TaskEntity cleanupTask,
      TableMetadata tableMetadata,
      FileIO fileIO,
      IcebergTableLikeEntity tableEntity,
      PolarisMetaStoreManager metaStoreManager,
      PolarisCallContext polarisCallContext) {
    // read the manifest list for each snapshot. dedupe the manifest files and schedule a
    // cleanupTask for each manifest file and its data files to be deleted
    Set<String> seenPaths = new HashSet<>();
    return tableMetadata.snapshots().stream()
        .flatMap(sn -> sn.allManifests(fileIO).stream())
        // distinct by manifest path, since multiple snapshots will contain the same manifest
        .filter(mf -> seenPaths.add(mf.path()))
        .filter(mf -> TaskUtils.exists(mf.path(), fileIO))
        .map(
            mf -> {
              // append a random uuid to the task name to avoid any potential conflict
              // when
              // storing the task entity. It's better to have duplicate tasks than to risk
              // not storing the rest of the task entities. If a duplicate deletion task
              // is
              // queued, it will check for the manifest file's existence and simply exit
              // if
              // the task has already been handled.
              String taskName = cleanupTask.getName() + "_" + mf.path() + "_" + UUID.randomUUID();
              LOGGER
                  .atDebug()
                  .addKeyValue("taskName", taskName)
                  .addKeyValue("tableIdentifier", tableEntity.getTableIdentifier())
                  .addKeyValue("metadataLocation", tableEntity.getMetadataLocation())
                  .addKeyValue("manifestFile", mf.path())
                  .log("Queueing task to delete manifest file");
              return new TaskEntity.Builder()
                  .setName(taskName)
                  .setId(metaStoreManager.generateNewEntityId(polarisCallContext).getId())
                  .setCreateTimestamp(clock.millis())
                  .withTaskType(AsyncTaskType.MANIFEST_FILE_CLEANUP)
                  .withData(
                      ManifestFileCleanupTaskHandler.ManifestCleanupTask.buildFrom(
                          tableEntity.getTableIdentifier(), mf))
                  .setId(metaStoreManager.generateNewEntityId(polarisCallContext).getId())
                  // copy the internal properties, which will have storage info
                  .setInternalProperties(cleanupTask.getInternalPropertiesAsMap())
                  .build();
            });
  }

  private Stream<TaskEntity> getMetadataTaskStream(
      TaskEntity cleanupTask,
      TableMetadata tableMetadata,
      IcebergTableLikeEntity tableEntity,
      PolarisMetaStoreManager metaStoreManager,
      CallContext callContext) {
    PolarisCallContext polarisCallContext = callContext.getPolarisCallContext();
    int batchSize = callContext.getRealmConfig().getConfig(TABLE_METADATA_CLEANUP_BATCH_SIZE);

    Iterator<String> metadataFiles =
        Stream.of(
                tableMetadata.previousFiles().stream().map(TableMetadata.MetadataLogEntry::file),
                tableMetadata.snapshots().stream().map(Snapshot::manifestListLocation),
                tableMetadata.statisticsFiles().stream().map(StatisticsFile::path),
                tableMetadata.partitionStatisticsFiles().stream()
                    .map(PartitionStatisticsFile::path))
            .flatMap(Function.identity())
            .iterator();

    // Create an iterator that yields TaskEntity batches
    return Stream.generate(
            () -> {
              List<String> metadataBatch = new ArrayList<>(batchSize);
              for (int i = 0; i < batchSize && metadataFiles.hasNext(); i++) {
                metadataBatch.add(metadataFiles.next());
              }
              return metadataBatch;
            })
        .takeWhile(batch -> !batch.isEmpty())
        .map(
            metadataBatch -> {
              String taskName =
                  String.join(
                      "_",
                      cleanupTask.getName(),
                      metadataBatch.toString(),
                      UUID.randomUUID().toString());
              LOGGER
                  .atDebug()
                  .addKeyValue("taskName", taskName)
                  .addKeyValue("tableIdentifier", tableEntity.getTableIdentifier())
                  .addKeyValue("metadataFiles", metadataBatch.toString())
                  .log(
                      "Queueing task to delete metadata files (prev metadata and statistics files)");
              return new TaskEntity.Builder()
                  .setName(taskName)
                  .setId(metaStoreManager.generateNewEntityId(polarisCallContext).getId())
                  .setCreateTimestamp(clock.millis())
                  .withTaskType(AsyncTaskType.BATCH_FILE_CLEANUP)
                  .withData(
                      new BatchFileCleanupTaskHandler.BatchFileCleanupTask(
                          tableEntity.getTableIdentifier(), metadataBatch))
                  .setInternalProperties(cleanupTask.getInternalPropertiesAsMap())
                  .build();
            });
  }
}

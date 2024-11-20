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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.TableLikeEntity;
import org.apache.polaris.core.entity.TaskEntity;
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
  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final Function<TaskEntity, FileIO> fileIOSupplier;
  private static final String BATCH_SIZE_CONFIG_KEY = "TABLE_METADATA_CLEANUP_BATCH_SIZE";

  public TableCleanupTaskHandler(
      TaskExecutor taskExecutor,
      MetaStoreManagerFactory metaStoreManagerFactory,
      Function<TaskEntity, FileIO> fileIOSupplier) {
    this.taskExecutor = taskExecutor;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.fileIOSupplier = fileIOSupplier;
  }

  @Override
  public boolean canHandleTask(TaskEntity task) {
    return task.getTaskType() == AsyncTaskType.ENTITY_CLEANUP_SCHEDULER && taskEntityIsTable(task);
  }

  private boolean taskEntityIsTable(TaskEntity task) {
    PolarisEntity entity = PolarisEntity.of((task.readData(PolarisBaseEntity.class)));
    return entity.getType().equals(PolarisEntityType.TABLE_LIKE);
  }

  @Override
  public boolean handleTask(TaskEntity cleanupTask) {
    PolarisBaseEntity entity = cleanupTask.readData(PolarisBaseEntity.class);
    PolarisMetaStoreManager metaStoreManager =
        metaStoreManagerFactory.getOrCreateMetaStoreManager(
            CallContext.getCurrentContext().getRealmContext());
    TableLikeEntity tableEntity = TableLikeEntity.of(entity);
    PolarisCallContext polarisCallContext = CallContext.getCurrentContext().getPolarisCallContext();
    LOGGER
        .atInfo()
        .addKeyValue("tableIdentifier", tableEntity.getTableIdentifier())
        .addKeyValue("metadataLocation", tableEntity.getMetadataLocation())
        .log("Handling table metadata cleanup task");

    // It's likely the cleanupTask has already been completed, but wasn't dropped successfully.
    // Log a
    // warning and move on
    try (FileIO fileIO = fileIOSupplier.apply(cleanupTask)) {
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

      Stream<TaskEntity> manifestCleanupTasks =
          getManifestTaskStream(
              cleanupTask,
              tableMetadata,
              fileIO,
              tableEntity,
              metaStoreManager,
              polarisCallContext);

      // TODO: handle partition statistics files
      Stream<TaskEntity> metadataFileCleanupTasks =
          getMetadataTaskStream(
              cleanupTask,
              tableMetadata,
              fileIO,
              tableEntity,
              metaStoreManager,
              polarisCallContext);

      List<TaskEntity> taskEntities =
          Stream.concat(manifestCleanupTasks, metadataFileCleanupTasks).toList();

      List<PolarisBaseEntity> createdTasks =
          metaStoreManager
              .createEntitiesIfNotExist(polarisCallContext, null, taskEntities)
              .getEntities();
      if (createdTasks != null) {
        LOGGER
            .atInfo()
            .addKeyValue("tableIdentifier", tableEntity.getTableIdentifier())
            .addKeyValue("metadataLocation", tableEntity.getMetadataLocation())
            .addKeyValue("taskCount", taskEntities.size())
            .log(
                "Successfully queued tasks to delete manifests, previous metadata, and statistics files - deleting table metadata file");
        for (PolarisBaseEntity createdTask : createdTasks) {
          taskExecutor.addTaskHandlerContext(createdTask.getId(), CallContext.getCurrentContext());
        }

        fileIO.deleteFile(tableEntity.getMetadataLocation());

        return true;
      }
    }
    return false;
  }

  private Stream<TaskEntity> getManifestTaskStream(
      TaskEntity cleanupTask,
      TableMetadata tableMetadata,
      FileIO fileIO,
      TableLikeEntity tableEntity,
      PolarisMetaStoreManager metaStoreManager,
      PolarisCallContext polarisCallContext) {
    // read the manifest list for each snapshot. dedupe the manifest files and schedule a
    // cleanupTask
    // for each manifest file and its data files to be deleted
    return tableMetadata.snapshots().stream()
        .flatMap(sn -> sn.allManifests(fileIO).stream())
        // distinct by manifest path, since multiple snapshots will contain the same
        // manifest
        .collect(Collectors.toMap(ManifestFile::path, Function.identity(), (mf1, mf2) -> mf1))
        .values()
        .stream()
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
                  .setCreateTimestamp(polarisCallContext.getClock().millis())
                  .withTaskType(AsyncTaskType.MANIFEST_FILE_CLEANUP)
                  .withData(
                      new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                          tableEntity.getTableIdentifier(), TaskUtils.encodeManifestFile(mf)))
                  .setId(metaStoreManager.generateNewEntityId(polarisCallContext).getId())
                  // copy the internal properties, which will have storage info
                  .setInternalProperties(cleanupTask.getInternalPropertiesAsMap())
                  .build();
            });
  }

  private Stream<TaskEntity> getMetadataTaskStream(
      TaskEntity cleanupTask,
      TableMetadata tableMetadata,
      FileIO fileIO,
      TableLikeEntity tableEntity,
      PolarisMetaStoreManager metaStoreManager,
      PolarisCallContext polarisCallContext) {
    int batchSize =
        polarisCallContext
            .getConfigurationStore()
            .getConfiguration(polarisCallContext, BATCH_SIZE_CONFIG_KEY, 10);
    return getMetadataFileBatches(tableMetadata, batchSize).stream()
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
                  .setCreateTimestamp(polarisCallContext.getClock().millis())
                  .withTaskType(AsyncTaskType.METADATA_FILE_BATCH_CLEANUP)
                  .withData(
                      new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                          tableEntity.getTableIdentifier(), metadataBatch))
                  .setInternalProperties(cleanupTask.getInternalPropertiesAsMap())
                  .build();
            });
  }

  private List<List<String>> getMetadataFileBatches(TableMetadata tableMetadata, int batchSize) {
    List<List<String>> result = new ArrayList<>();
    List<String> metadataFiles =
        Stream.concat(
                tableMetadata.previousFiles().stream().map(TableMetadata.MetadataLogEntry::file),
                tableMetadata.statisticsFiles().stream().map(StatisticsFile::path))
            .toList();

    for (int i = 0; i < metadataFiles.size(); i += batchSize) {
      result.add(metadataFiles.subList(i, Math.min(i + batchSize, metadataFiles.size())));
    }
    return result;
  }
}

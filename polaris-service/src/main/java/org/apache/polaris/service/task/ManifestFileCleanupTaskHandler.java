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

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import org.apache.commons.codec.binary.Base64;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.TaskEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link TaskHandler} responsible for deleting all of the files in a manifest and the manifest
 * itself. Since data files may be present in multiple manifests across different snapshots, we
 * assume a data file that doesn't exist is missing because it was already deleted by another task.
 */
public class ManifestFileCleanupTaskHandler implements TaskHandler {
  public static final int MAX_ATTEMPTS = 3;
  public static final int FILE_DELETION_RETRY_MILLIS = 100;
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ManifestFileCleanupTaskHandler.class);
  private final Function<TaskEntity, FileIO> fileIOSupplier;
  private final ExecutorService executorService;

  public ManifestFileCleanupTaskHandler(
      Function<TaskEntity, FileIO> fileIOSupplier, ExecutorService executorService) {
    this.fileIOSupplier = fileIOSupplier;
    this.executorService = executorService;
  }

  @Override
  public boolean canHandleTask(TaskEntity task) {
    return task.getTaskType() == AsyncTaskType.FILE_CLEANUP;
  }

  @Override
  public boolean handleTask(TaskEntity task) {
    ManifestCleanupTask cleanupTask = task.readData(ManifestCleanupTask.class);
    ManifestFile manifestFile = decodeManifestData(cleanupTask.getManifestFileData());
    TableIdentifier tableId = cleanupTask.getTableId();
    try (FileIO authorizedFileIO = fileIOSupplier.apply(task)) {

      // if the file doesn't exist, we assume that another task execution was successful, but failed
      // to drop the task entity. Log a warning and return success
      if (!TaskUtils.exists(manifestFile.path(), authorizedFileIO)) {
        LOGGER
            .atWarn()
            .addKeyValue("manifestFile", manifestFile.path())
            .addKeyValue("tableId", tableId)
            .log("Manifest cleanup task scheduled, but manifest file doesn't exist");
        return true;
      }

      ManifestReader<DataFile> dataFiles = ManifestFiles.read(manifestFile, authorizedFileIO);
      List<CompletableFuture<Void>> dataFileDeletes =
          StreamSupport.stream(
                  Spliterators.spliteratorUnknownSize(dataFiles.iterator(), Spliterator.IMMUTABLE),
                  false)
              .map(
                  file ->
                      tryDelete(
                          tableId, authorizedFileIO, manifestFile, file.path().toString(), null, 1))
              .toList();
      LOGGER.debug(
          "Scheduled {} data files to be deleted from manifest {}",
          dataFileDeletes.size(),
          manifestFile.path());
      try {
        // wait for all data files to be deleted, then wait for the manifest itself to be deleted
        CompletableFuture.allOf(dataFileDeletes.toArray(CompletableFuture[]::new))
            .thenCompose(
                (v) -> {
                  LOGGER
                      .atInfo()
                      .addKeyValue("manifestFile", manifestFile.path())
                      .log("All data files in manifest deleted - deleting manifest");
                  return tryDelete(
                      tableId, authorizedFileIO, manifestFile, manifestFile.path(), null, 1);
                })
            .get();
        return true;
      } catch (InterruptedException e) {
        LOGGER.error(
            "Interrupted exception deleting data files from manifest {}", manifestFile.path(), e);
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        LOGGER.error("Unable to delete data files from manifest {}", manifestFile.path(), e);
        return false;
      }
    }
  }

  private static ManifestFile decodeManifestData(String manifestFileData) {
    try {
      return ManifestFiles.decode(Base64.decodeBase64(manifestFileData));
    } catch (IOException e) {
      throw new RuntimeException("Unable to decode base64 encoded manifest", e);
    }
  }

  private CompletableFuture<Void> tryDelete(
      TableIdentifier tableId,
      FileIO fileIO,
      ManifestFile manifestFile,
      String dataFile,
      Throwable e,
      int attempt) {
    if (e != null && attempt <= MAX_ATTEMPTS) {
      LOGGER
          .atWarn()
          .addKeyValue("dataFile", dataFile)
          .addKeyValue("attempt", attempt)
          .addKeyValue("error", e.getMessage())
          .log("Error encountered attempting to delete data file");
    }
    if (attempt > MAX_ATTEMPTS && e != null) {
      return CompletableFuture.failedFuture(e);
    }
    return CompletableFuture.runAsync(
            () -> {
              // totally normal for a file to already be missing, as a data file
              // may be in multiple manifests. There's a possibility we check the
              // file's existence, but then it is deleted before we have a chance to
              // send the delete request. In such a case, we <i>should</i> retry
              // and find
              if (TaskUtils.exists(dataFile, fileIO)) {
                fileIO.deleteFile(dataFile);
              } else {
                LOGGER
                    .atInfo()
                    .addKeyValue("dataFile", dataFile)
                    .addKeyValue("manifestFile", manifestFile.path())
                    .addKeyValue("tableId", tableId)
                    .log("Manifest cleanup task scheduled, but data file doesn't exist");
              }
            },
            executorService)
        .exceptionallyComposeAsync(
            newEx -> {
              LOGGER
                  .atWarn()
                  .addKeyValue("dataFile", dataFile)
                  .addKeyValue("tableIdentifer", tableId)
                  .addKeyValue("manifestFile", manifestFile.path())
                  .log("Exception caught deleting data file from manifest", newEx);
              return tryDelete(tableId, fileIO, manifestFile, dataFile, newEx, attempt + 1);
            },
            CompletableFuture.delayedExecutor(
                FILE_DELETION_RETRY_MILLIS, TimeUnit.MILLISECONDS, executorService));
  }

  /** Serialized Task data sent from the {@link TableCleanupTaskHandler} */
  public static final class ManifestCleanupTask {
    private TableIdentifier tableId;
    private String manifestFileData;

    public ManifestCleanupTask(TableIdentifier tableId, String manifestFileData) {
      this.tableId = tableId;
      this.manifestFileData = manifestFileData;
    }

    public ManifestCleanupTask() {}

    public TableIdentifier getTableId() {
      return tableId;
    }

    public void setTableId(TableIdentifier tableId) {
      this.tableId = tableId;
    }

    public String getManifestFileData() {
      return manifestFileData;
    }

    public void setManifestFileData(String manifestFileData) {
      this.manifestFileData = manifestFileData;
    }

    @Override
    public boolean equals(Object object) {
      if (this == object) return true;
      if (!(object instanceof ManifestCleanupTask that)) return false;
      return Objects.equals(tableId, that.tableId)
          && Objects.equals(manifestFileData, that.manifestFileData);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableId, manifestFileData);
    }
  }
}

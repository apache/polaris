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
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.StreamSupport;
import org.apache.commons.codec.binary.Base64;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.TaskEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ManifestFileCleanupTaskHandler} responsible for deleting all the files in a manifest and
 * the manifest itself. Since data files may be present in multiple manifests across different
 * snapshots, we assume a data file that doesn't exist is missing because it was already deleted by
 * another task.
 */
public class ManifestFileCleanupTaskHandler extends FileCleanupTaskHandler {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ManifestFileCleanupTaskHandler.class);

  public ManifestFileCleanupTaskHandler(
      TaskFileIOSupplier fileIOSupplier, ExecutorService executorService) {
    super(fileIOSupplier, executorService);
  }

  @Override
  public boolean canHandleTask(TaskEntity task) {
    return task.getTaskType() == AsyncTaskType.MANIFEST_FILE_CLEANUP;
  }

  @Override
  public boolean handleTask(TaskEntity task, CallContext callContext) {
    ManifestCleanupTask cleanupTask = task.readData(ManifestCleanupTask.class);
    TableIdentifier tableId = cleanupTask.tableId();
    try (FileIO authorizedFileIO = fileIOSupplier.apply(task, tableId, callContext)) {
      ManifestFile manifestFile = decodeManifestData(cleanupTask.manifestFileData());
      return cleanUpManifestFile(manifestFile, authorizedFileIO, tableId);
    }
  }

  private boolean cleanUpManifestFile(
      ManifestFile manifestFile, FileIO fileIO, TableIdentifier tableId) {
    // if the file doesn't exist, we assume that another task execution was successful, but
    // failed to drop the task entity. Log a warning and return success
    if (!TaskUtils.exists(manifestFile.path(), fileIO)) {
      LOGGER
          .atWarn()
          .addKeyValue("manifestFile", manifestFile.path())
          .addKeyValue("tableId", tableId)
          .log("Manifest cleanup task scheduled, but manifest file doesn't exist");
      return true;
    }

    ManifestReader<DataFile> dataFiles = ManifestFiles.read(manifestFile, fileIO);
    List<CompletableFuture<Void>> dataFileDeletes =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(dataFiles.iterator(), Spliterator.IMMUTABLE),
                false)
            .map(file -> tryDelete(tableId, fileIO, manifestFile.path(), file.location(), null, 1))
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
                    tableId, fileIO, manifestFile.path(), manifestFile.path(), null, 1);
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

  private static ManifestFile decodeManifestData(String manifestFileData) {
    try {
      return ManifestFiles.decode(Base64.decodeBase64(manifestFileData));
    } catch (IOException e) {
      throw new RuntimeException("Unable to decode base64 encoded manifest", e);
    }
  }

  /** Serialized Task data sent from the {@link TableCleanupTaskHandler} */
  public record ManifestCleanupTask(TableIdentifier tableId, String manifestFileData) {}
  ;
}

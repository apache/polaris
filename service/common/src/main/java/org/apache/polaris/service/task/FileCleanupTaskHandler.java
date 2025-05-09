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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.TaskEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for handling file cleanup tasks within Apache Polaris.
 *
 * <p>This class is for performing asynchronous file deletions with retry logic.
 *
 * <p>Subclasses must implement {@link #canHandleTask(TaskEntity)} and {@link
 * #handleTask(TaskEntity, CallContext)} to define task-specific handling logic.
 */
public abstract class FileCleanupTaskHandler implements TaskHandler {

  public static final int MAX_ATTEMPTS = 3;
  public static final int FILE_DELETION_RETRY_MILLIS = 100;
  public final TaskFileIOSupplier fileIOSupplier;
  public final ExecutorService executorService;
  private static final Logger LOGGER = LoggerFactory.getLogger(FileCleanupTaskHandler.class);

  public FileCleanupTaskHandler(
      TaskFileIOSupplier fileIOSupplier, ExecutorService executorService) {
    this.fileIOSupplier = fileIOSupplier;
    this.executorService = executorService;
  }

  @Override
  public abstract boolean canHandleTask(TaskEntity task);

  @Override
  public abstract boolean handleTask(TaskEntity task, CallContext callContext);

  /**
   * Attempts to delete a single file with retry logic. If the file does not exist, it logs a
   * message and does not retry. If an error occurs, it retries up to {@link #MAX_ATTEMPTS} times
   * before failing.
   *
   * @param tableId The identifier of the table associated with the file.
   * @param fileIO The {@link FileIO} instance used for file operations.
   * @param baseFile An optional base file associated with the file being deleted (can be null).
   * @param file The path of the file to be deleted.
   * @param e The exception from the previous attempt, if any.
   * @param attempt The current retry attempt count.
   * @return A {@link CompletableFuture} representing the asynchronous deletion operation.
   */
  public CompletableFuture<Void> tryDelete(
      TableIdentifier tableId,
      FileIO fileIO,
      String baseFile,
      String file,
      Throwable e,
      int attempt) {
    if (e != null && attempt <= MAX_ATTEMPTS) {
      LOGGER
          .atWarn()
          .addKeyValue("file", file)
          .addKeyValue("attempt", attempt)
          .addKeyValue("error", e.getMessage())
          .log("Error encountered attempting to delete file");
    }
    if (attempt > MAX_ATTEMPTS && e != null) {
      return CompletableFuture.failedFuture(e);
    }
    return CompletableFuture.runAsync(
            () -> {
              // totally normal for a file to already be missing, e.g. a data file
              // may be in multiple manifests. There's a possibility we check the
              // file's existence, but then it is deleted before we have a chance to
              // send the delete request. In such a case, we <i>should</i> retry
              // and find
              if (TaskUtils.exists(file, fileIO)) {
                fileIO.deleteFile(file);
              } else {
                LOGGER
                    .atInfo()
                    .addKeyValue("file", file)
                    .addKeyValue("baseFile", baseFile != null ? baseFile : "")
                    .addKeyValue("tableId", tableId)
                    .log("table file cleanup task scheduled, but data file doesn't exist");
              }
            },
            executorService)
        .exceptionallyComposeAsync(
            newEx -> {
              LOGGER
                  .atWarn()
                  .addKeyValue("file", file)
                  .addKeyValue("tableIdentifier", tableId)
                  .addKeyValue("baseFile", baseFile != null ? baseFile : "")
                  .log("Exception caught deleting data file", newEx);
              return tryDelete(tableId, fileIO, baseFile, file, newEx, attempt + 1);
            },
            CompletableFuture.delayedExecutor(
                FILE_DELETION_RETRY_MILLIS, TimeUnit.MILLISECONDS, executorService));
  }

  /**
   * Attempts to delete multiple files in a batch operation with retry logic. If an error occurs, it
   * retries up to {@link #MAX_ATTEMPTS} times before failing.
   *
   * @param tableId The identifier of the table associated with the files.
   * @param fileIO The {@link FileIO} instance used for file operations.
   * @param files The list of file paths to be deleted.
   * @param type The type of files being deleted (e.g., data files, metadata files).
   * @param isConcurrent Whether the deletion should be performed concurrently.
   * @param e The exception from the previous attempt, if any.
   * @param attempt The current retry attempt count.
   * @return A {@link CompletableFuture} representing the asynchronous batch deletion operation.
   */
  public CompletableFuture<Void> tryDelete(
      TableIdentifier tableId,
      FileIO fileIO,
      Iterable<String> files,
      String type,
      Boolean isConcurrent,
      Throwable e,
      int attempt) {
    if (e != null && attempt <= MAX_ATTEMPTS) {
      LOGGER
          .atWarn()
          .addKeyValue("files", files)
          .addKeyValue("attempt", attempt)
          .addKeyValue("error", e.getMessage())
          .addKeyValue("type", type)
          .log("Error encountered attempting to delete files");
    }
    if (attempt > MAX_ATTEMPTS && e != null) {
      return CompletableFuture.failedFuture(e);
    }
    return CompletableFuture.runAsync(
            () -> CatalogUtil.deleteFiles(fileIO, files, type, isConcurrent), executorService)
        .exceptionallyComposeAsync(
            newEx -> {
              LOGGER
                  .atWarn()
                  .addKeyValue("files", files)
                  .addKeyValue("tableIdentifier", tableId)
                  .addKeyValue("type", type)
                  .log("Exception caught deleting data files", newEx);
              return tryDelete(tableId, fileIO, files, type, isConcurrent, newEx, attempt + 1);
            },
            CompletableFuture.delayedExecutor(
                FILE_DELETION_RETRY_MILLIS, TimeUnit.MILLISECONDS, executorService));
  }
}

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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.TaskEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link FileCleanupTaskHandler} responsible for cleaning up files in table tasks. Handles retries
 * for file deletions and skips files that are already missing. Subclasses must implement
 * task-specific logic.
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
  public abstract boolean handleTask(
      RealmContext realmContext, RealmConfig realmConfig, TaskEntity task);

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
}

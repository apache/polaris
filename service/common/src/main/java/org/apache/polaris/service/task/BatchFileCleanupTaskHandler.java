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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.TaskEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link BatchFileCleanupTaskHandler} responsible for batch file cleanup by processing multiple
 * file deletions in a single task handler. Valid files are deleted asynchronously with retries for
 * transient errors, while missing files are logged and skipped.
 */
public class BatchFileCleanupTaskHandler extends FileCleanupTaskHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchFileCleanupTaskHandler.class);

  public BatchFileCleanupTaskHandler(
      TaskFileIOSupplier fileIOSupplier, ExecutorService executorService) {
    super(fileIOSupplier, executorService);
  }

  @Override
  public boolean canHandleTask(TaskEntity task) {
    return task.getTaskType() == AsyncTaskType.BATCH_FILE_CLEANUP;
  }

  @Override
  public boolean handleTask(TaskEntity task, CallContext callContext) {
    BatchFileCleanupTask cleanupTask = task.readData(BatchFileCleanupTask.class);
    TableIdentifier tableId = cleanupTask.tableId();
    List<String> batchFiles = cleanupTask.batchFiles();
    try (FileIO authorizedFileIO = fileIOSupplier.apply(task, callContext)) {
      List<String> validFiles =
          batchFiles.stream().filter(file -> TaskUtils.exists(file, authorizedFileIO)).toList();
      if (validFiles.isEmpty()) {
        LOGGER
            .atWarn()
            .addKeyValue("batchFiles", batchFiles.toString())
            .addKeyValue("tableId", tableId)
            .log("File batch cleanup task scheduled, but none of the files in batch exists");
        return true;
      }
      if (validFiles.size() < batchFiles.size()) {
        List<String> missingFiles =
            batchFiles.stream().filter(file -> !TaskUtils.exists(file, authorizedFileIO)).toList();
        LOGGER
            .atWarn()
            .addKeyValue("batchFiles", batchFiles.toString())
            .addKeyValue("missingFiles", missingFiles.toString())
            .addKeyValue("tableId", tableId)
            .log(
                "File batch cleanup task scheduled, but {} files in the batch are missing",
                missingFiles.size());
      }

      CompletableFuture<Void> deleteFutures =
          tryDelete(tableId, authorizedFileIO, validFiles, cleanupTask.type().getValue(), true, null, 1);

      try {
        deleteFutures.join();
      } catch (Exception e) {
        LOGGER.error("Exception detected during batch files deletion", e);
        return false;
      }

      return true;
    }
  }

  public enum BatchFileType {
    TABLE_METADATA("table_metadata");

    private final String value;

    BatchFileType(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  public record BatchFileCleanupTask(
      TableIdentifier tableId, List<String> batchFiles, BatchFileType type) {}
}

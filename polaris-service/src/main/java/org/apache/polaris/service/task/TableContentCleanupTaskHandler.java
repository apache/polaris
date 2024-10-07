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

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.TaskEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * {@link TaskHandler} responsible for deleting previous metadata and statistics files of a table.
 */
public class TableContentCleanupTaskHandler implements TaskHandler {
    public static final int MAX_ATTEMPTS = 3;
    public static final int FILE_DELETION_RETRY_MILLIS = 100;
    private static final Logger LOGGER =
            LoggerFactory.getLogger(TableContentCleanupTaskHandler.class);
    private final Function<TaskEntity, FileIO> fileIOSupplier;
    private final ExecutorService executorService;

    public TableContentCleanupTaskHandler(Function<TaskEntity, FileIO> fileIOSupplier,
                                          ExecutorService executorService) {
        this.fileIOSupplier = fileIOSupplier;
        this.executorService = executorService;
    }

    @Override
    public boolean canHandleTask(TaskEntity task) {
        return task.getTaskType() == AsyncTaskType.TABLE_CONTENT_CLEANUP;
    }

    @Override
    public boolean handleTask(TaskEntity task) {
        TableContentCleanupTask cleanupTask = task.readData(TableContentCleanupTask.class);
        List<String> fileBatch = cleanupTask.getFileBatch();
        TableIdentifier tableId = cleanupTask.getTableId();
        try (FileIO authorizedFileIO = fileIOSupplier.apply(task)) {
            List<String> validFiles = fileBatch.stream()
                    .filter(file -> TaskUtils.exists(file, authorizedFileIO))
                    .toList();
            if (validFiles.isEmpty()) {
                LOGGER.atWarn()
                        .addKeyValue("taskName", task.getName())
                        .addKeyValue("fileBatch", fileBatch.toString())
                        .addKeyValue("tableId", tableId)
                        .log("Table content cleanup task scheduled, but the none of the file in batch exists");
                return true;
            }

            // Schedule the deletion for each file asynchronously
            List<CompletableFuture<Void>> deleteFutures = validFiles.stream()
                    .map(file -> tryDelete(tableId, authorizedFileIO, file, null, 1))
                    .toList();

            // Wait for all delete operations to finish
            CompletableFuture<Void> allDeletes = CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture[0]));
            allDeletes.join();

            LOGGER.atInfo()
                    .addKeyValue("taskName", task.getName())
                    .addKeyValue("fileBatch", fileBatch.toString())
                    .addKeyValue("tableId", tableId)
                    .log("All the files in task have been deleted");

            return true;
        } catch (Exception e) {
            LOGGER.error("Error during table content cleanup for file batch {}", fileBatch.toString(), e);
            return false;
        }
    }

    private CompletableFuture<Void> tryDelete(TableIdentifier tableId,
                                              FileIO fileIO,
                                              String filePath,
                                              Throwable e,
                                              int attempt) {
        if (e != null && attempt <= MAX_ATTEMPTS) {
            LOGGER.atWarn()
                    .addKeyValue("filePath", filePath)
                    .addKeyValue("attempt", attempt)
                    .addKeyValue("error", e.getMessage())
                    .log("Error encountered attempting to delete file");
        }

        if (attempt > MAX_ATTEMPTS && e != null) {
            return CompletableFuture.failedFuture(e);
        }

        return CompletableFuture.runAsync(() -> {
            if (TaskUtils.exists(filePath, fileIO)) {
                fileIO.deleteFile(filePath);
                LOGGER.atInfo()
                        .addKeyValue("filePath", filePath)
                        .addKeyValue("tableId", tableId)
                        .addKeyValue("attempt", attempt)
                        .log("Successfully deleted file {}", filePath);
            } else {
                LOGGER.atInfo()
                        .addKeyValue("filePath", filePath)
                        .addKeyValue("tableId", tableId)
                        .log("File doesn't exist, likely already deleted");
            }
        }, executorService).exceptionallyComposeAsync(
                newEx -> {
                    LOGGER.atWarn()
                            .addKeyValue("filePath", filePath)
                            .addKeyValue("tableId", tableId)
                            .log("Exception caught deleting table content file", newEx);
                    return tryDelete(tableId, fileIO, filePath, newEx, attempt + 1);
                },
                CompletableFuture.delayedExecutor(FILE_DELETION_RETRY_MILLIS, TimeUnit.MILLISECONDS, executorService)
        );
    }

    public static final class TableContentCleanupTask {
        private TableIdentifier tableId;
        private List<String> fileBatch;

        public TableContentCleanupTask() {
        }

        public TableContentCleanupTask(TableIdentifier tableId, List<String> fileBatch) {
            this.tableId = tableId;
            this.fileBatch = fileBatch;
        }

        public TableIdentifier getTableId() {
            return tableId;
        }

        public void setTableId(TableIdentifier tableId) {
            this.tableId = tableId;
        }

        public List<String> getFileBatch() {
            return fileBatch;
        }

        public void setFileBatch(List<String> fileBatch) {
            this.fileBatch = fileBatch;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof TableContentCleanupTask other)) {
                return false;
            }
            return Objects.equals(tableId, other.tableId) && Objects.equals(fileBatch, other.fileBatch);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableId, fileBatch.toString());
        }
    }
}
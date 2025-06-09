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

package org.apache.polaris.service.events.listeners;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.polaris.core.entity.PolarisEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FileBufferListingTask implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileBufferListingTask.class);
  private final String bufferDirectory;
  private final Function<TaskSubmissionInput, Future> taskSubmitter;
  private final BiConsumer<String, Integer> rotateShard;
  private final BiConsumer<String, List<PolarisEvent>> eventWriter;
  private final ConcurrentHashMap<String, Future> activeFlushFutures;
  private final int timeToFlush;

  public FileBufferListingTask(
      String bufferDirectory,
      Function<TaskSubmissionInput, Future> taskSubmitter,
      ConcurrentHashMap<String, Future> activeFlushFutures,
      int timeToFlush,
      BiConsumer<String, Integer> rotateShard,
      BiConsumer<String, List<PolarisEvent>> eventWriter) {
    this.bufferDirectory = bufferDirectory;
    this.taskSubmitter = taskSubmitter;
    this.activeFlushFutures = activeFlushFutures;
    this.timeToFlush = timeToFlush;
    this.rotateShard = rotateShard;
    this.eventWriter = eventWriter;
  }

  @Override
  public void run() {
    LOGGER.info("Attempting to list all event buffer files");
    try {
      // Example, if `topLevelDirectory` is /var/tmp/...
      File topLevelDirectory = new File(this.bufferDirectory);
      File[] realmDirs = topLevelDirectory.listFiles();
      if (realmDirs == null || realmDirs.length == 0) {
        LOGGER.debug(
            "No event buffer files found in path: {}", topLevelDirectory.getAbsolutePath());
        return;
      }
      for (File realmDir : realmDirs) {
        // Then `realmDir` would be something like /var/tmp/POLARIS/
        String realmId = realmDir.getName();
        LOGGER.debug("Buffer realm directory found for realmId: {}", realmId);
        ArrayList<String> filesToFlush = new ArrayList<>();
        File[] bufferDirs = realmDir.listFiles();
        if (bufferDirs == null || bufferDirs.length == 0) {
          LOGGER.debug("No buffer directories found in path: {}", realmDir.getAbsolutePath());
          continue;
        }
        for (File bufferDir : bufferDirs) {
          // And `bufferDir` would be something like /var/tmp/POLARIS/polaris-event-buffer-shard-1/
          File[] bufferFiles = bufferDir.listFiles();
          if (bufferFiles == null || bufferFiles.length == 0) {
            LOGGER.debug("No buffer files found in path: {}", bufferDir.getAbsolutePath());
            continue;
          }
          // Get all buffer files except for the latest one
          File latestFile = Arrays.stream(bufferFiles).max(File::compareTo).orElse(null);
          Arrays.stream(bufferFiles)
              .map(File::getAbsolutePath)
              .filter(
                  file ->
                      !file.equals(latestFile.getAbsolutePath())
                          && !activeFlushFutures.containsKey(file))
              .forEach(filesToFlush::add);

          // Get the buffer start time from the file name
          String[] fileDeconstructed = latestFile.getName().split("-");
          // Check if this buffer has been alive for longer than the `timeToFlush`
          if (System.currentTimeMillis()
                  - Long.parseLong(fileDeconstructed[fileDeconstructed.length - 1])
              > timeToFlush) {
            // Get realmId and buffer number
            String[] shardDirNameDeconstructed = bufferDir.getName().split("-");
            int bufferNum =
                Integer.parseInt(shardDirNameDeconstructed[shardDirNameDeconstructed.length - 1]);

            // Create a new shard (file and writer)
            this.rotateShard.accept(realmId, bufferNum);

            // Add this file to filesToFlush
            filesToFlush.add(latestFile.getAbsolutePath());
          }
        }
        for (String file : filesToFlush) {
          FileFlushTask task =
              new FileFlushTask(
                  file, realmDir.getName(), activeFlushFutures::remove, this.eventWriter);
          activeFlushFutures.computeIfAbsent(
              file, k -> taskSubmitter.apply(new TaskSubmissionInput(task, 0)));
        }
      }
    } finally {
      var future = taskSubmitter.apply(new TaskSubmissionInput(this, timeToFlush));
    }
  }

  public record TaskSubmissionInput(Runnable task, Integer delayInMs) {}
}

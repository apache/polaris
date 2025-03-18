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
package org.apache.polaris.service.quarkus.task;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatPredicate;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.annotation.Nonnull;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.codec.binary.Base64;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.task.ManifestFileCleanupTaskHandler;
import org.apache.polaris.service.task.TaskFileIOSupplier;
import org.apache.polaris.service.task.TaskUtils;
import org.junit.jupiter.api.Test;

@QuarkusTest
class ManifestFileCleanupTaskHandlerTest {
  @Inject MetaStoreManagerFactory metaStoreManagerFactory;

  private final RealmContext realmContext = () -> "realmName";

  private TaskFileIOSupplier buildTaskFileIOSupplier(FileIO fileIO) {
    return new TaskFileIOSupplier(
        new FileIOFactory() {
          @Override
          public FileIO loadFileIO(
              @Nonnull CallContext callContext,
              @Nonnull String ioImplClassName,
              @Nonnull Map<String, String> properties,
              @Nonnull TableIdentifier identifier,
              @Nonnull Set<String> tableLocations,
              @Nonnull Set<PolarisStorageActions> storageActions,
              @Nonnull PolarisResolvedPathWrapper resolvedEntityPath) {
            return fileIO;
          }
        });
  }

  private void addTaskLocation(TaskEntity task) {
    Map<String, String> internalPropertiesAsMap = new HashMap<>(task.getInternalPropertiesAsMap());
    internalPropertiesAsMap.put(PolarisTaskConstants.STORAGE_LOCATION, "file:///tmp/");
    ((PolarisBaseEntity) task).setInternalPropertiesAsMap(internalPropertiesAsMap);
  }

  @Test
  public void testCleanupFileNotExists() throws IOException {
    PolarisCallContext polarisCallContext =
        new PolarisCallContext(
            metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get(),
            new PolarisDefaultDiagServiceImpl());
    try (CallContext callCtx = CallContext.of(realmContext, polarisCallContext)) {
      CallContext.setCurrentContext(callCtx);
      FileIO fileIO = new InMemoryFileIO();
      TableIdentifier tableIdentifier =
          TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");

      ManifestFileCleanupTaskHandler handler =
          new ManifestFileCleanupTaskHandler(
              buildTaskFileIOSupplier(fileIO), Executors.newSingleThreadExecutor());
      ManifestFile manifestFile =
          TaskTestUtils.manifestFile(
              fileIO, "manifest1.avro", 1L, "dataFile1.parquet", "dataFile2.parquet");
      fileIO.deleteFile(manifestFile.path());
      TaskEntity task =
          new TaskEntity.Builder()
              .withTaskType(AsyncTaskType.MANIFEST_FILE_CLEANUP)
              .withData(
                  new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                      tableIdentifier,
                      Base64.encodeBase64String(ManifestFiles.encode(manifestFile))))
              .setName(UUID.randomUUID().toString())
              .build();
      addTaskLocation(task);
      assertThatPredicate(handler::canHandleTask).accepts(task);
      assertThat(handler.handleTask(task, callCtx)).isTrue();
    }
  }

  @Test
  public void testCleanupFileManifestExistsDataFilesDontExist() throws IOException {
    PolarisCallContext polarisCallContext =
        new PolarisCallContext(
            metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get(),
            new PolarisDefaultDiagServiceImpl());
    try (CallContext callCtx = CallContext.of(realmContext, polarisCallContext)) {
      CallContext.setCurrentContext(callCtx);
      FileIO fileIO = new InMemoryFileIO();
      TableIdentifier tableIdentifier =
          TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
      ManifestFileCleanupTaskHandler handler =
          new ManifestFileCleanupTaskHandler(
              buildTaskFileIOSupplier(fileIO), Executors.newSingleThreadExecutor());
      ManifestFile manifestFile =
          TaskTestUtils.manifestFile(
              fileIO, "manifest1.avro", 100L, "dataFile1.parquet", "dataFile2.parquet");
      TaskEntity task =
          new TaskEntity.Builder()
              .withTaskType(AsyncTaskType.MANIFEST_FILE_CLEANUP)
              .withData(
                  new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                      tableIdentifier,
                      Base64.encodeBase64String(ManifestFiles.encode(manifestFile))))
              .setName(UUID.randomUUID().toString())
              .build();
      addTaskLocation(task);
      assertThatPredicate(handler::canHandleTask).accepts(task);
      assertThat(handler.handleTask(task, callCtx)).isTrue();
    }
  }

  @Test
  public void testCleanupFiles() throws IOException {
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
      ManifestFileCleanupTaskHandler handler =
          new ManifestFileCleanupTaskHandler(
              buildTaskFileIOSupplier(fileIO), Executors.newSingleThreadExecutor());
      String dataFile1Path = "dataFile1.parquet";
      OutputFile dataFile1 = fileIO.newOutputFile(dataFile1Path);
      PositionOutputStream out1 = dataFile1.createOrOverwrite();
      out1.write("the data".getBytes(UTF_8));
      out1.close();
      String dataFile2Path = "dataFile2.parquet";
      OutputFile dataFile2 = fileIO.newOutputFile(dataFile2Path);
      PositionOutputStream out2 = dataFile2.createOrOverwrite();
      out2.write("the data".getBytes(UTF_8));
      out2.close();
      ManifestFile manifestFile =
          TaskTestUtils.manifestFile(fileIO, "manifest1.avro", 100L, dataFile1Path, dataFile2Path);
      TaskEntity task =
          new TaskEntity.Builder()
              .withTaskType(AsyncTaskType.MANIFEST_FILE_CLEANUP)
              .withData(
                  new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                      tableIdentifier,
                      Base64.encodeBase64String(ManifestFiles.encode(manifestFile))))
              .setName(UUID.randomUUID().toString())
              .build();
      addTaskLocation(task);
      assertThatPredicate(handler::canHandleTask).accepts(task);
      assertThat(handler.handleTask(task, callCtx)).isTrue();
      assertThatPredicate((String f) -> TaskUtils.exists(f, fileIO)).rejects(dataFile1Path);
      assertThatPredicate((String f) -> TaskUtils.exists(f, fileIO)).rejects(dataFile2Path);
    }
  }

  @Test
  public void testCleanupFilesWithRetries() throws IOException {
    PolarisCallContext polarisCallContext =
        new PolarisCallContext(
            metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get(),
            new PolarisDefaultDiagServiceImpl());
    try (CallContext callCtx = CallContext.of(realmContext, polarisCallContext)) {
      CallContext.setCurrentContext(callCtx);
      Map<String, AtomicInteger> retryCounter = new HashMap<>();
      FileIO fileIO =
          new InMemoryFileIO() {
            @Override
            public void close() {
              // no-op
            }

            @Override
            public void deleteFile(String location) {
              int attempts =
                  retryCounter
                      .computeIfAbsent(location, k -> new AtomicInteger(0))
                      .incrementAndGet();
              if (attempts < 3) {
                throw new RuntimeException("I'm failing to test retries");
              } else {
                // succeed on the third attempt
                super.deleteFile(location);
              }
            }
          };

      TableIdentifier tableIdentifier =
          TableIdentifier.of(Namespace.of("db1", "schema1"), "table1");
      ManifestFileCleanupTaskHandler handler =
          new ManifestFileCleanupTaskHandler(
              buildTaskFileIOSupplier(fileIO), Executors.newSingleThreadExecutor());
      String dataFile1Path = "dataFile1.parquet";
      OutputFile dataFile1 = fileIO.newOutputFile(dataFile1Path);
      PositionOutputStream out1 = dataFile1.createOrOverwrite();
      out1.write("the data".getBytes(UTF_8));
      out1.close();
      String dataFile2Path = "dataFile2.parquet";
      OutputFile dataFile2 = fileIO.newOutputFile(dataFile2Path);
      PositionOutputStream out2 = dataFile2.createOrOverwrite();
      out2.write("the data".getBytes(UTF_8));
      out2.close();
      ManifestFile manifestFile =
          TaskTestUtils.manifestFile(fileIO, "manifest1.avro", 100L, dataFile1Path, dataFile2Path);
      TaskEntity task =
          new TaskEntity.Builder()
              .withTaskType(AsyncTaskType.MANIFEST_FILE_CLEANUP)
              .withData(
                  new ManifestFileCleanupTaskHandler.ManifestCleanupTask(
                      tableIdentifier,
                      Base64.encodeBase64String(ManifestFiles.encode(manifestFile))))
              .setName(UUID.randomUUID().toString())
              .build();
      addTaskLocation(task);
      assertThatPredicate(handler::canHandleTask).accepts(task);
      assertThat(handler.handleTask(task, callCtx)).isTrue();
      assertThatPredicate((String f) -> TaskUtils.exists(f, fileIO)).rejects(dataFile1Path);
      assertThatPredicate((String f) -> TaskUtils.exists(f, fileIO)).rejects(dataFile2Path);
    }
  }
}

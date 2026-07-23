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

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.PolarisEncryptionUtil;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;

@RequestScoped
public class TaskFileIOSupplier {
  private final FileIOFactory fileIOFactory;
  private final StorageAccessConfigProvider accessConfigProvider;

  @Inject
  public TaskFileIOSupplier(
      FileIOFactory fileIOFactory, StorageAccessConfigProvider storageAccessConfigProvider) {
    this.fileIOFactory = fileIOFactory;
    this.accessConfigProvider = storageAccessConfigProvider;
  }

  public FileIO apply(TaskEntity task, TableIdentifier identifier) {
    Map<String, String> taskProperties = task.getInternalPropertiesAsMap();
    // Task properties carry both raw FileIO settings and the task-only encryption context.
    // Initialize the raw FileIO from a filtered copy; retain the original for wrapping it below.
    Map<String, String> fileIOProperties = new HashMap<>(taskProperties);
    fileIOProperties.remove(PolarisTaskConstants.ENCRYPTION_CONTEXT);

    String location = fileIOProperties.get(PolarisTaskConstants.STORAGE_LOCATION);
    Set<String> locations = Set.of(location);
    Set<PolarisStorageActions> storageActions = Set.of(PolarisStorageActions.ALL);
    ResolvedPolarisEntity resolvedTaskEntity =
        new ResolvedPolarisEntity(task, List.of(), List.of());
    PolarisResolvedPathWrapper resolvedPath =
        new PolarisResolvedPathWrapper(List.of(resolvedTaskEntity));
    StorageAccessConfig storageAccessConfig =
        accessConfigProvider.getStorageAccessConfig(
            identifier, locations, storageActions, Optional.empty(), resolvedPath);

    String ioImpl =
        fileIOProperties.getOrDefault(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.io.ResolvingFileIO");

    FileIO fileIO = fileIOFactory.loadFileIO(storageAccessConfig, ioImpl, fileIOProperties);
    try {
      return PolarisEncryptionUtil.encryptTaskFileIO(fileIO, taskProperties);
    } catch (RuntimeException e) {
      try {
        fileIO.close();
      } catch (RuntimeException closeException) {
        e.addSuppressed(closeException);
      }
      throw e;
    }
  }
}

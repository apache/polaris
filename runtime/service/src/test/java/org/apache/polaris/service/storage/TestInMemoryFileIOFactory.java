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
package org.apache.polaris.service.storage;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;
import java.util.Map;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.jspecify.annotations.NonNull;

/**
 * A {@code FileIO} factory selected by {@code polaris.file-io.type=test-in-memory}: every table
 * write and read in these CDI tests goes through {@link InMemoryFileIO}, never a real cloud
 * endpoint, keeping the S3 credential vending mechanism tests hermetic. Selected through the same
 * {@code @Identifier}-and-config mechanism as {@code DefaultFileIOFactory} ({@code
 * ServiceProducers.fileIOFactory}), so no CDI alternative is needed: {@code
 * CatalogProperties.FILE_IO_IMPL} is never set on any catalog or request, so {@code
 * IcebergPropertiesValidation.determineFileIOClassName}'s insecure-storage-type check (the one that
 * would otherwise require {@code ALLOW_INSECURE_STORAGE_TYPES}, a severe readiness issue) never
 * runs; the storage config still reports the real, safe {@code S3FileIO} class name, but this
 * factory substitutes {@link InMemoryFileIO} underneath it regardless.
 */
@RequestScoped
@Identifier("test-in-memory")
public class TestInMemoryFileIOFactory implements FileIOFactory {

  @Override
  public FileIO loadFileIO(
      @NonNull StorageAccessConfig storageAccessConfig,
      @NonNull String ioImplClassName,
      @NonNull Map<String, String> properties) {
    return new InMemoryFileIO();
  }
}

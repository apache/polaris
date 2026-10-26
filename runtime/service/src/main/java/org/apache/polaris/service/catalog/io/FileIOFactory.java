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
package org.apache.polaris.service.catalog.io;

import jakarta.enterprise.context.RequestScoped;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.jspecify.annotations.NonNull;

/**
 * Interface for providing a way to construct FileIO objects, such as for reading/writing S3.
 *
 * <p>Implementations are available via CDI as {@link RequestScoped @RequestScoped} beans.
 */
public interface FileIOFactory {

  /**
   * Loads a FileIO implementation for server-side use (metadata read/write, purge).
   *
   * <p>This method may obtain subscoped credentials to restrict the FileIO's permissions, ensuring
   * secure and limited access to the table's data and locations.
   *
   * <p>{@code storageAccessConfig} is authoritative for credentials and storage-config extras.
   * {@code properties} may carry catalog-trusted contextual settings (for example {@code
   * table-default.*}). Callers must not pass table {@code metadata.properties()}, which can include
   * caller-controlled FileIO client settings such as {@code s3.endpoint}.
   *
   * @param storageAccessConfig the storage access configuration containing credentials and other
   *     properties.
   * @param ioImplClassName the class name of the FileIO implementation to load.
   * @param properties catalog-trusted contextual properties for the FileIO.
   * @return a configured FileIO instance.
   */
  FileIO loadFileIO(
      @NonNull StorageAccessConfig storageAccessConfig,
      @NonNull String ioImplClassName,
      @NonNull Map<String, String> properties);
}

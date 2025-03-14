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

import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.PolarisStorageActions;

/**
 * Interface for providing a way to construct FileIO objects, such as for reading/writing S3.
 *
 * <p>Implementations are available via CDI as {@link ApplicationScoped @ApplicationScoped} beans.
 */
public interface FileIOFactory {

  /**
   * Loads a FileIO implementation for a specific table in the given realm with detailed config.
   *
   * <p>This method may obtain subscoped credentials to restrict the FileIO's permissions, ensuring
   * secure and limited access to the table's data and locations.
   *
   * @param callContext the call for which the FileIO is being loaded.
   * @param ioImplClassName the class name of the FileIO implementation to load.
   * @param properties configuration properties for the FileIO.
   * @param identifier the table identifier.
   * @param tableLocations locations associated with the table.
   * @param storageActions storage actions allowed for the table.
   * @param resolvedEntityPath resolved paths for the entities.
   * @return a configured FileIO instance.
   */
  FileIO loadFileIO(
      @Nonnull CallContext callContext,
      @Nonnull String ioImplClassName,
      @Nonnull Map<String, String> properties,
      @Nonnull TableIdentifier identifier,
      @Nonnull Set<String> tableLocations,
      @Nonnull Set<PolarisStorageActions> storageActions,
      @Nonnull PolarisResolvedPathWrapper resolvedEntityPath);
}

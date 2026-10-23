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
package org.apache.polaris.service.catalog.iceberg;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.storage.StorageLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tuple capturing the state needed to delete metadata files that were removed by a table commit.
 * Used to defer cleanup until a multi-table transaction has been atomically committed.
 *
 * @param tableLocations the table locations recorded on the table entity before the commit
 */
record MetadataFileCleanup(
    FileIO io, TableMetadata baseMetadata, TableMetadata newMetadata, Set<String> tableLocations) {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataFileCleanup.class);

  /**
   * Same as {@link CatalogUtil#deleteRemovedMetadataFiles}, except that only files under {@link
   * #tableLocations()} are deleted.
   */
  void deleteRemovedMetadataFiles() {
    if (baseMetadata == null
        || !newMetadata.propertyAsBoolean(
            TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
            TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT)) {
      return;
    }
    List<StorageLocation> locations = tableLocations.stream().map(StorageLocation::of).toList();
    Set<TableMetadata.MetadataLogEntry> removed = new HashSet<>(baseMetadata.previousFiles());
    removed.removeAll(newMetadata.previousFiles());
    Set<String> files = new HashSet<>();
    for (TableMetadata.MetadataLogEntry entry : removed) {
      StorageLocation file = StorageLocation.of(entry.file());
      if (locations.stream().anyMatch(file::isChildOf)) {
        files.add(entry.file());
      } else {
        LOGGER.warn(
            "Not deleting metadata file {} outside of the table locations {}",
            entry.file(),
            tableLocations);
      }
    }
    CatalogUtil.deleteFiles(io, files, "metadata");
  }
}

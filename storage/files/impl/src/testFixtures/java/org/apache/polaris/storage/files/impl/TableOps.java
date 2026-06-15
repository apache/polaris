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

package org.apache.polaris.storage.files.impl;

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;

/** Helper for Iceberg integration tests. */
class TableOps extends BaseMetastoreTableOperations {
  private final ConcurrentMap<TableIdentifier, String> tables;
  private final FileIO fileIO;
  private final TableIdentifier tableIdentifier;

  TableOps(
      ConcurrentMap<TableIdentifier, String> tables,
      FileIO fileIO,
      TableIdentifier tableIdentifier) {
    this.tables = tables;
    this.fileIO = fileIO;
    this.tableIdentifier = tableIdentifier;
  }

  @Override
  protected void doRefresh() {
    var latestLocation = tables.get(tableIdentifier);
    if (latestLocation == null) {
      disableRefresh();
    } else {
      refreshFromMetadataLocation(latestLocation);
    }
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    var newLocation = writeNewMetadataIfRequired(base == null, metadata);
    var oldLocation = base == null ? null : base.metadataFileLocation();
    tables.compute(
        tableIdentifier,
        (k, existingLocation) -> {
          if (!Objects.equals(existingLocation, oldLocation)) {
            if (null == base) {
              throw new AlreadyExistsException("Table already exists: %s", tableName());
            }

            if (null == existingLocation) {
              throw new NoSuchTableException("Table does not exist: %s", tableName());
            }

            throw new CommitFailedException(
                "Cannot commit to table %s metadata location from %s to %s "
                    + "because it has been concurrently modified to %s",
                tableIdentifier, oldLocation, newLocation, existingLocation);
          }
          return newLocation;
        });
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected String tableName() {
    return "";
  }
}

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
package org.apache.polaris.persistence.relational.jdbc;

import com.google.common.base.Preconditions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.sql.SQLException;
import java.time.Instant;

/**
 * Deletes rows from the {@code events} table that are older than a cutoff.
 *
 * <p>Rows are deleted in batches, each in its own transaction, so that a purge of a large table
 * does not hold locks or a transaction open for its whole duration. An interrupted purge can simply
 * be run again.
 */
@ApplicationScoped
public class JdbcEventsPurger {

  private final DatasourceOperations datasourceOperations;

  @Inject
  public JdbcEventsPurger(DatasourceOperations datasourceOperations) {
    this.datasourceOperations = datasourceOperations;
  }

  /**
   * Deletes the events of {@code realmId} whose timestamp is strictly before {@code cutoff}.
   *
   * @return the number of deleted events
   */
  public long purgeEventsBefore(String realmId, Instant cutoff, int batchSize) {
    Preconditions.checkArgument(batchSize > 0, "Batch size must be positive");
    datasourceOperations.validateSchemaCompatibility();
    var query =
        QueryGenerator.generateDeleteEventsBeforeQuery(realmId, cutoff.toEpochMilli(), batchSize);
    long total = 0;
    try {
      int deleted;
      do {
        deleted = datasourceOperations.executeUpdate(query);
        total += deleted;
      } while (deleted == batchSize);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Failed to purge events of realm %s after deleting %d events: %s",
              realmId, total, e.getMessage()),
          e);
    }
    return total;
  }
}

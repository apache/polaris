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

import java.sql.SQLException;
import java.util.List;
import org.apache.polaris.persistence.relational.jdbc.QueryGenerator.PreparedQuery;

/**
 * A {@link RealmDataPurger} that deletes rows from a single realm-scoped table in bounded batches.
 *
 * <p>Uses a portable {@code DELETE ... WHERE realm_id = ? AND <key> IN (SELECT <key> ... WHERE
 * realm_id = ? LIMIT ?)}: the {@code LIMIT} lives inside a subquery because {@code DELETE ...
 * LIMIT} is not supported by PostgreSQL (it is by CockroachDB and H2), and the outer {@code
 * realm_id} predicate keeps the delete scoped to the realm even when the key column is only unique
 * within a realm (e.g. metrics {@code report_id}).
 */
public class TableRealmDataPurger implements RealmDataPurger {

  private final DatasourceOperations datasourceOperations;
  private final String tableName;
  private final String keyColumn;

  /**
   * @param tableName realm-scoped table to purge
   * @param keyColumn a column that is unique within a realm (used to bound each batch)
   */
  public TableRealmDataPurger(
      DatasourceOperations datasourceOperations, String tableName, String keyColumn) {
    this.datasourceOperations = datasourceOperations;
    this.tableName = tableName;
    this.keyColumn = keyColumn;
  }

  @Override
  public String storeName() {
    return tableName;
  }

  @Override
  public long purgeBatch(String realmId, int batchSize) {
    String fqTable = QueryGenerator.getFullyQualifiedTableName(tableName);
    String sql =
        "DELETE FROM "
            + fqTable
            + " WHERE realm_id = ? AND "
            + keyColumn
            + " IN (SELECT "
            + keyColumn
            + " FROM "
            + fqTable
            + " WHERE realm_id = ? LIMIT ?)";
    try {
      return datasourceOperations.executeUpdate(
          new PreparedQuery(sql, List.of(realmId, realmId, batchSize)));
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Failed to purge batch from %s for realm %s due to %s",
              tableName, realmId, e.getMessage()),
          e);
    }
  }
}

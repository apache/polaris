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
package org.apache.polaris.persistence.relational.jdbc.models;

import java.util.Map;
import java.util.Set;

/**
 * Aggregates schema-level metadata across the {@code Model*} classes.
 *
 * <p>Currently the registry exposes the set of JSON-typed columns per table. JSON columns require
 * database-specific WHERE-clause placeholder handling (MySQL needs {@code CAST(? AS JSON)} for
 * structural equality), and {@code QueryGenerator} consults this registry instead of inspecting
 * parameter values at runtime.
 */
public final class ModelRegistry {

  private static final Map<String, Set<String>> JSON_COLUMNS_BY_TABLE =
      Map.of(
          ModelEntity.TABLE_NAME, ModelEntity.JSON_COLUMNS,
          ModelEvent.TABLE_NAME, ModelEvent.JSON_COLUMNS,
          ModelPolicyMappingRecord.TABLE_NAME, ModelPolicyMappingRecord.JSON_COLUMNS,
          ModelCommitMetricsReport.TABLE_NAME, ModelCommitMetricsReport.JSON_COLUMNS,
          ModelScanMetricsReport.TABLE_NAME, ModelScanMetricsReport.JSON_COLUMNS);

  private ModelRegistry() {}

  /**
   * Returns {@code true} when {@code columnName} is declared as a JSON column on the table named
   * {@code tableName}. Tables that have no JSON columns (or are not registered) return {@code
   * false}.
   */
  public static boolean isJsonColumn(String tableName, String columnName) {
    Set<String> jsonColumns = JSON_COLUMNS_BY_TABLE.get(tableName);
    return jsonColumns != null && jsonColumns.contains(columnName);
  }
}

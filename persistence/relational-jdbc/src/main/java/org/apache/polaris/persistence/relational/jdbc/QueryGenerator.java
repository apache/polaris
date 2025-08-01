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

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEntity;
import org.apache.polaris.persistence.relational.jdbc.models.ModelGrantRecord;

/**
 * Utility class to generate parameterized SQL queries (SELECT, INSERT, UPDATE, DELETE). Ensures
 * consistent SQL generation and protects against injection by managing parameters separately.
 */
public class QueryGenerator {

  /** A container for the SQL string and the ordered parameter values. */
  public record PreparedQuery(String sql, List<Object> parameters) {}

  /** A container for the query fragment SQL string and the ordered parameter values. */
  record QueryFragment(String sql, List<Object> parameters) {}

  /**
   * Generates a SELECT query with projection and filtering.
   *
   * @param projections List of columns to retrieve.
   * @param tableName Target table name.
   * @param whereClause Column-value pairs used in WHERE filtering.
   * @return A parameterized SELECT query.
   * @throws IllegalArgumentException if any whereClause column isn't in projections.
   */
  public static PreparedQuery generateSelectQuery(
      @Nonnull List<String> projections,
      @Nonnull String tableName,
      @Nonnull Map<String, Object> whereClause) {
    return generateSelectQuery(projections, tableName, whereClause, Map.of(), null);
  }

  /**
   * Generates a SELECT query with projection and filtering.
   *
   * @param projections List of columns to retrieve.
   * @param tableName Target table name.
   * @param whereEquals Column-value pairs used in WHERE filtering.
   * @return A parameterized SELECT query.
   * @throws IllegalArgumentException if any whereClause column isn't in projections.
   */
  public static PreparedQuery generateSelectQuery(
      @Nonnull List<String> projections,
      @Nonnull String tableName,
      @Nonnull Map<String, Object> whereEquals,
      @Nonnull Map<String, Object> whereGreater,
      @Nullable String orderByColumn) {
    QueryFragment where =
        generateWhereClause(new HashSet<>(projections), whereEquals, whereGreater);
    PreparedQuery query = generateSelectQuery(projections, tableName, where.sql(), orderByColumn);
    return new PreparedQuery(query.sql(), where.parameters());
  }

  /**
   * Builds a DELETE query to remove grant records for a given entity.
   *
   * @param entity The target entity (either grantee or securable).
   * @param realmId The associated realm.
   * @return A DELETE query removing all grants for this entity.
   */
  public static PreparedQuery generateDeleteQueryForEntityGrantRecords(
      @Nonnull PolarisEntityCore entity, @Nonnull String realmId) {
    String where =
        """
             WHERE (
                (grantee_id = ? AND grantee_catalog_id = ?) OR
                (securable_id = ? AND securable_catalog_id = ?)
            ) AND realm_id = ?""";
    List<Object> params =
        Arrays.asList(
            entity.getId(), entity.getCatalogId(), entity.getId(), entity.getCatalogId(), realmId);
    return new PreparedQuery(
        "DELETE FROM " + getFullyQualifiedTableName(ModelGrantRecord.TABLE_NAME) + where, params);
  }

  /**
   * Builds a SELECT query using a list of entity ID pairs (catalog_id, id).
   *
   * @param realmId Realm to filter by.
   * @param entityIds List of PolarisEntityId pairs.
   * @return SELECT query to retrieve matching entities.
   * @throws IllegalArgumentException if entityIds is empty.
   */
  public static PreparedQuery generateSelectQueryWithEntityIds(
      @Nonnull String realmId, @Nonnull List<PolarisEntityId> entityIds) {
    if (entityIds.isEmpty()) {
      throw new IllegalArgumentException("Empty entity ids");
    }
    String placeholders = entityIds.stream().map(e -> "(?, ?)").collect(Collectors.joining(", "));
    List<Object> params = new ArrayList<>();
    for (PolarisEntityId id : entityIds) {
      params.add(id.getCatalogId());
      params.add(id.getId());
    }
    params.add(realmId);
    String where = " WHERE (catalog_id, id) IN (" + placeholders + ") AND realm_id = ?";
    return new PreparedQuery(
        generateSelectQuery(ModelEntity.ALL_COLUMNS, ModelEntity.TABLE_NAME, where, null).sql(),
        params);
  }

  /**
   * Generates an INSERT query for a given table.
   *
   * @param allColumns Columns to insert values into.
   * @param tableName Target table name.
   * @param values Values for each column (must match order of columns).
   * @param realmId Realm value to append.
   * @return INSERT query with value bindings.
   */
  public static PreparedQuery generateInsertQuery(
      @Nonnull List<String> allColumns,
      @Nonnull String tableName,
      List<Object> values,
      String realmId) {
    List<String> finalColumns = new ArrayList<>(allColumns);
    List<Object> finalValues = new ArrayList<>(values);
    finalColumns.add("realm_id");
    finalValues.add(realmId);
    String columns = String.join(", ", finalColumns);
    String placeholders = finalColumns.stream().map(c -> "?").collect(Collectors.joining(", "));
    String sql =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " ("
            + columns
            + ") VALUES ("
            + placeholders
            + ")";
    return new PreparedQuery(sql, finalValues);
  }

  /**
   * Builds an UPDATE query.
   *
   * @param allColumns Columns to update.
   * @param tableName Target table.
   * @param values New values (must match columns in order).
   * @param whereClause Conditions for filtering rows to update.
   * @return UPDATE query with parameter values.
   */
  public static PreparedQuery generateUpdateQuery(
      @Nonnull List<String> allColumns,
      @Nonnull String tableName,
      @Nonnull List<Object> values,
      @Nonnull Map<String, Object> whereClause) {
    List<Object> bindingParams = new ArrayList<>(values);
    QueryFragment where = generateWhereClause(new HashSet<>(allColumns), whereClause, Map.of());
    String setClause = allColumns.stream().map(c -> c + " = ?").collect(Collectors.joining(", "));
    String sql =
        "UPDATE " + getFullyQualifiedTableName(tableName) + " SET " + setClause + where.sql();
    bindingParams.addAll(where.parameters());
    return new PreparedQuery(sql, bindingParams);
  }

  /**
   * Builds a DELETE query with the given conditions.
   *
   * @param tableColumns List of valid table columns.
   * @param tableName Target table.
   * @param whereClause Column-value filters.
   * @return DELETE query with parameter bindings.
   */
  public static PreparedQuery generateDeleteQuery(
      @Nonnull List<String> tableColumns,
      @Nonnull String tableName,
      @Nonnull Map<String, Object> whereClause) {
    QueryFragment where = generateWhereClause(new HashSet<>(tableColumns), whereClause, Map.of());
    return new PreparedQuery(
        "DELETE FROM " + getFullyQualifiedTableName(tableName) + where.sql(), where.parameters());
  }

  private static PreparedQuery generateSelectQuery(
      @Nonnull List<String> columnNames,
      @Nonnull String tableName,
      @Nonnull String filter,
      @Nullable String orderByColumn) {
    String sql =
        "SELECT "
            + String.join(", ", columnNames)
            + " FROM "
            + getFullyQualifiedTableName(tableName)
            + filter;
    if (orderByColumn != null) {
      sql += " ORDER BY " + orderByColumn + " ASC";
    }
    return new PreparedQuery(sql, Collections.emptyList());
  }

  @VisibleForTesting
  static QueryFragment generateWhereClause(
      @Nonnull Set<String> tableColumns,
      @Nonnull Map<String, Object> whereEquals,
      @Nonnull Map<String, Object> whereGreater) {
    List<String> conditions = new ArrayList<>();
    List<Object> parameters = new ArrayList<>();
    for (Map.Entry<String, Object> entry : whereEquals.entrySet()) {
      if (!tableColumns.contains(entry.getKey()) && !entry.getKey().equals("realm_id")) {
        throw new IllegalArgumentException("Invalid query column: " + entry.getKey());
      }
      conditions.add(entry.getKey() + " = ?");
      parameters.add(entry.getValue());
    }
    for (Map.Entry<String, Object> entry : whereGreater.entrySet()) {
      if (!tableColumns.contains(entry.getKey()) && !entry.getKey().equals("realm_id")) {
        throw new IllegalArgumentException("Invalid query column: " + entry.getKey());
      }
      conditions.add(entry.getKey() + " > ?");
      parameters.add(entry.getValue());
    }
    String clause = conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions);
    return new QueryFragment(clause, parameters);
  }

  @VisibleForTesting
  static PreparedQuery generateVersionQuery() {
    return new PreparedQuery("SELECT version_value FROM POLARIS_SCHEMA.VERSION", List.of());
  }

  /**
   * Generate a SELECT query to find any entities that have a given realm &amp; parent and that may
   * overlap with a given location. The check is performed without consideration for the scheme, so
   * a path on one storage type may give a false positive for overlapping with another storage type.
   * This should be combined with a check using `StorageLocation`.
   *
   * @param realmId A realm to search within
   * @param catalogId A catalog entity to search within
   * @param baseLocation The base location to look for overlap with, with or without a scheme
   * @return The list of possibly overlapping entities that meet the criteria
   */
  @VisibleForTesting
  public static PreparedQuery generateOverlapQuery(
      String realmId, long catalogId, String baseLocation) {
    StorageLocation baseStorageLocation = StorageLocation.of(baseLocation);
    String locationWithoutScheme = baseStorageLocation.withoutScheme();

    List<String> conditions = new ArrayList<>();
    List<Object> parameters = new ArrayList<>();

    String[] components = locationWithoutScheme.split("/");
    StringBuilder pathBuilder = new StringBuilder();

    for (String component : components) {
      pathBuilder.append(component).append("/");
      conditions.add("location_without_scheme = ?");
      parameters.add(pathBuilder.toString());
    }

    // Add LIKE condition to match children
    conditions.add("location_without_scheme LIKE ?");
    parameters.add(locationWithoutScheme + "%");

    String locationClause = String.join(" OR ", conditions);
    String clause = " WHERE realm_id = ? AND catalog_id = ? AND (" + locationClause + ")";

    // realmId and parentId go first
    List<Object> finalParams = new ArrayList<>();
    finalParams.add(realmId);
    finalParams.add(catalogId);
    finalParams.addAll(parameters);

    QueryFragment where = new QueryFragment(clause, finalParams);
    PreparedQuery query =
        generateSelectQuery(ModelEntity.ALL_COLUMNS, ModelEntity.TABLE_NAME, where.sql(), null);
    return new PreparedQuery(query.sql(), where.parameters());
  }

  private static String getFullyQualifiedTableName(String tableName) {
    // TODO: make schema name configurable.
    return "POLARIS_SCHEMA." + tableName;
  }
}

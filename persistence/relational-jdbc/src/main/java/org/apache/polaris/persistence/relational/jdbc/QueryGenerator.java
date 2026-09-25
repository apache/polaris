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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.tag.TargetField;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEntity;
import org.apache.polaris.persistence.relational.jdbc.models.ModelGrantRecord;
import org.apache.polaris.persistence.relational.jdbc.models.ModelTagAssignmentRecord;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Utility class to generate parameterized SQL queries (SELECT, INSERT, UPDATE, DELETE). Ensures
 * consistent SQL generation and protects against injection by managing parameters separately.
 *
 * <p>Generated queries reference tables by their unqualified names; the schema holding the Polaris
 * tables is selected through the datasource configuration (for example the PostgreSQL driver's
 * {@code currentSchema} connection property), so the persistence code is agnostic of it.
 */
public class QueryGenerator {

  /** A container for the SQL string and the ordered parameter values. */
  public record PreparedQuery(String sql, List<Object> parameters) {}

  /** A container for the SQL string and a list of the ordered parameter values. */
  public record PreparedBatchQuery(String sql, List<List<Object>> parametersList) {}

  /** A container for the query fragment SQL string and the ordered parameter values. */
  record QueryFragment(String sql, List<Object> parameters) {}

  /**
   * The full identity of a tag assignment row, in significance order. Used to order a bounded read
   * of the assignment table so that reading the same rows twice returns the same rows in the same
   * order.
   */
  private static final String TAG_ASSIGNMENT_IDENTITY_ORDER =
      "target_catalog_id, target_id, field_id, tag_catalog_id, tag_id";

  /**
   * Generates a SELECT query with projection and filtering.
   *
   * @param projections List of columns to retrieve.
   * @param tableName Target table name.
   * @param whereClause Column-value pairs used in WHERE filtering.
   * @return A parameterized SELECT query.
   * @throws IllegalArgumentException if any whereClause column isn't in projections or if limit is
   *     not positive.
   */
  public static PreparedQuery generateSelectQuery(
      @NonNull List<String> projections,
      @NonNull String tableName,
      @NonNull Map<String, Object> whereClause) {
    return generateSelectQuery(projections, tableName, whereClause, Map.of(), null);
  }

  /**
   * Generates a SELECT query bounded by a row limit. Useful for existence checks that only need to
   * know whether any matching row exists, avoiding fetching and materializing the full result set.
   *
   * @param projections List of columns to retrieve.
   * @param tableName Target table name.
   * @param whereClause Column-value pairs used in WHERE filtering.
   * @param limit Maximum number of rows to return.
   * @return A parameterized SELECT query with a LIMIT clause.
   * @throws IllegalArgumentException if any whereClause column isn't in projections.
   */
  public static PreparedQuery generateSelectQuery(
      @NonNull List<String> projections,
      @NonNull String tableName,
      @NonNull Map<String, Object> whereClause,
      int limit) {
    QueryFragment where = generateWhereClause(new HashSet<>(projections), whereClause, Map.of());
    PreparedQuery query = generateSelectQuery(projections, tableName, where.sql(), null, limit);
    return new PreparedQuery(query.sql(), where.parameters());
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
      @NonNull List<String> projections,
      @NonNull String tableName,
      @NonNull Map<String, Object> whereEquals,
      @NonNull Map<String, Object> whereGreater,
      @Nullable String orderByColumn) {
    return generateSelectQuery(
        projections, tableName, whereEquals, whereGreater, orderByColumn, null);
  }

  /**
   * Generates a SELECT query with projection, filtering, ordering and an optional row limit.
   *
   * @param projections List of columns to retrieve.
   * @param tableName Target table name.
   * @param whereEquals Column-value pairs used in WHERE filtering.
   * @param whereGreater Column-value pairs the row must be strictly greater than.
   * @param orderByColumn Column to order by, or null for no ordering.
   * @param limit Maximum number of rows to return, or null for no limit.
   * @return A parameterized SELECT query.
   * @throws IllegalArgumentException if any whereClause column isn't in projections or if limit is
   *     not positive.
   */
  public static PreparedQuery generateSelectQuery(
      @NonNull List<String> projections,
      @NonNull String tableName,
      @NonNull Map<String, Object> whereEquals,
      @NonNull Map<String, Object> whereGreater,
      @Nullable String orderByColumn,
      @Nullable Integer limit) {
    QueryFragment where =
        generateWhereClause(new HashSet<>(projections), whereEquals, whereGreater);
    PreparedQuery query =
        generateSelectQuery(projections, tableName, where.sql(), orderByColumn, limit);
    return new PreparedQuery(query.sql(), where.parameters());
  }

  /**
   * Builds a keyset-resumable SELECT: equality filters plus an optional strictly-greater row-value
   * resume over {@code resumeColumns}, ordered by those columns. Used by paginated scans whose
   * continuation key is composite, e.g. {@code (target_id, field_id)} for tag-assignment reverse
   * lookups.
   *
   * @param projections Columns to select.
   * @param tableName Target table.
   * @param whereEquals Column-value pairs the row must match exactly.
   * @param resumeColumns Columns forming the ordering and resume key, in significance order.
   * @param resumeValues Values of the resume key from the last row of the previous page; empty for
   *     the first page.
   * @param limit Max rows, or null for no limit.
   * @return SELECT query resuming strictly after the resume key in resume-column order.
   */
  public static PreparedQuery generateSelectQueryWithRowValueResume(
      @NonNull List<String> projections,
      @NonNull String tableName,
      @NonNull Map<String, Object> whereEquals,
      @NonNull List<String> resumeColumns,
      @NonNull List<Object> resumeValues,
      @Nullable Integer limit) {
    if (resumeColumns.isEmpty()) {
      throw new IllegalArgumentException("Resume columns must not be empty");
    }
    if (!resumeValues.isEmpty() && resumeValues.size() != resumeColumns.size()) {
      throw new IllegalArgumentException("Resume values must match resume columns");
    }
    Set<String> tableColumns = new HashSet<>(projections);
    validateColumns(tableColumns, new HashSet<>(resumeColumns));
    QueryFragment equalsWhere = generateWhereClause(tableColumns, whereEquals, Map.of());
    StringBuilder where = new StringBuilder(equalsWhere.sql());
    List<Object> parameters = new ArrayList<>(equalsWhere.parameters());
    if (!resumeValues.isEmpty()) {
      // Lexicographic strictly-greater over the composite key:
      // (c0 > ?) OR (c0 = ? AND c1 > ?) OR ...
      List<String> terms = new ArrayList<>();
      for (int i = 0; i < resumeColumns.size(); i++) {
        StringBuilder term = new StringBuilder("(");
        for (int j = 0; j < i; j++) {
          term.append(resumeColumns.get(j)).append(" = ? AND ");
          parameters.add(resumeValues.get(j));
        }
        term.append(resumeColumns.get(i)).append(" > ?)");
        parameters.add(resumeValues.get(i));
        terms.add(term.toString());
      }
      where.append(where.length() == 0 ? " WHERE " : " AND ");
      where.append("(").append(String.join(" OR ", terms)).append(")");
    }
    PreparedQuery query =
        generateSelectQuery(
            projections, tableName, where.toString(), String.join(", ", resumeColumns), limit);
    return new PreparedQuery(query.sql(), parameters);
  }

  /**
   * Builds a DELETE query to remove grant records for a given entity.
   *
   * @param entity The target entity (either grantee or securable).
   * @param realmId The associated realm.
   * @return A DELETE query removing all grants for this entity.
   */
  public static PreparedQuery generateDeleteQueryForEntityGrantRecords(
      @NonNull PolarisEntityCore entity, @NonNull String realmId) {
    String where =
        """
             WHERE (
                (grantee_id = ? AND grantee_catalog_id = ?) OR
                (securable_id = ? AND securable_catalog_id = ?)
            ) AND realm_id = ?""";
    List<Object> params =
        Arrays.asList(
            entity.getId(), entity.getCatalogId(), entity.getId(), entity.getCatalogId(), realmId);
    return new PreparedQuery("DELETE FROM " + ModelGrantRecord.TABLE_NAME + where, params);
  }

  /**
   * Builds a SELECT query using a list of entity ID pairs (catalog_id, id).
   *
   * @param realmId Realm to filter by.
   * @param schemaVersion The schema version of entities table to query
   * @param entityIds List of PolarisEntityId pairs.
   * @return SELECT query to retrieve matching entities.
   * @throws IllegalArgumentException if entityIds is empty.
   */
  public static PreparedQuery generateSelectQueryWithEntityIds(
      @NonNull String realmId, int schemaVersion, @NonNull List<PolarisEntityId> entityIds) {
    return generateSelectQueryWithEntityIds(
        realmId, ModelEntity.getAllColumnNames(schemaVersion), entityIds);
  }

  /**
   * Like {@link #generateSelectQueryWithEntityIds(String, int, List)} but selects only {@link
   * ModelEntity#VERSION_COLUMNS}. Used by {@code lookupEntityVersions} to avoid fetching large JSON
   * property blobs on the cache-validation hot path.
   */
  public static PreparedQuery generateSelectQueryWithEntityIdsVersionOnly(
      @NonNull String realmId, @NonNull List<PolarisEntityId> entityIds) {
    return generateSelectQueryWithEntityIds(realmId, ModelEntity.VERSION_COLUMNS, entityIds);
  }

  private static PreparedQuery generateSelectQueryWithEntityIds(
      @NonNull String realmId,
      @NonNull List<String> columns,
      @NonNull List<PolarisEntityId> entityIds) {
    if (entityIds.isEmpty()) {
      throw new IllegalArgumentException("Empty entity ids");
    }
    String placeholders = entityIds.stream().map(e -> "(?, ?)").collect(Collectors.joining(", "));
    List<Object> params = new ArrayList<>();
    for (PolarisEntityId id : entityIds) {
      params.add(id.catalogId());
      params.add(id.id());
    }
    params.add(realmId);
    String where = " WHERE (catalog_id, id) IN (" + placeholders + ") AND realm_id = ?";
    return new PreparedQuery(
        generateSelectQuery(columns, ModelEntity.TABLE_NAME, where, null).sql(), params);
  }

  /**
   * Builds a SELECT query over the tag assignment record table using a composite-tuple IN on {@code
   * (target_catalog_id, target_id, field_id)}. One statement covering every requested target field,
   * so the caller reads every level of a hierarchy under one snapshot instead of issuing one round
   * trip per level.
   *
   * <p>A bounded read is ordered by the record identity as well as limited. Without an order, two
   * reads of the same rows may each return a different subset of them, and a caller that compares
   * the two to establish that they describe one state would see a disagreement that no writer
   * caused.
   *
   * @param realmId Realm to filter by.
   * @param targetFields List of (targetCatalogId, targetId, fieldId) keys.
   * @param limit Max rows, or null for no limit.
   * @return SELECT query to retrieve matching tag assignment records.
   * @throws IllegalArgumentException if targetFields is empty.
   */
  public static PreparedQuery generateSelectQueryWithTargetFields(
      @NonNull String realmId, @NonNull List<TargetField> targetFields, @Nullable Integer limit) {
    if (targetFields.isEmpty()) {
      throw new IllegalArgumentException("Empty target fields");
    }
    String placeholders =
        targetFields.stream().map(t -> "(?, ?, ?)").collect(Collectors.joining(", "));
    List<Object> params = new ArrayList<>();
    for (TargetField targetField : targetFields) {
      params.add(targetField.targetCatalogId());
      params.add(targetField.targetId());
      params.add(targetField.fieldId());
    }
    params.add(realmId);
    String where =
        " WHERE (target_catalog_id, target_id, field_id) IN ("
            + placeholders
            + ") AND realm_id = ?";
    return new PreparedQuery(
        generateSelectQuery(
                ModelTagAssignmentRecord.ALL_COLUMNS,
                ModelTagAssignmentRecord.TABLE_NAME,
                where,
                limit == null ? null : TAG_ASSIGNMENT_IDENTITY_ORDER,
                limit)
            .sql(),
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
      @NonNull List<String> allColumns,
      @NonNull String tableName,
      List<Object> values,
      String realmId) {
    List<String> finalColumns = new ArrayList<>(allColumns);
    List<Object> finalValues = new ArrayList<>(values);
    finalColumns.add("realm_id");
    finalValues.add(realmId);
    String columns = String.join(", ", finalColumns);
    String placeholders = finalColumns.stream().map(c -> "?").collect(Collectors.joining(", "));
    String sql = "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + placeholders + ")";
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
      @NonNull List<String> allColumns,
      @NonNull String tableName,
      @NonNull List<Object> values,
      @NonNull Map<String, Object> whereClause) {
    List<Object> bindingParams = new ArrayList<>(values);
    QueryFragment where = generateWhereClause(new HashSet<>(allColumns), whereClause, Map.of());
    String setClause = allColumns.stream().map(c -> c + " = ?").collect(Collectors.joining(", "));
    String sql = "UPDATE " + tableName + " SET " + setClause + where.sql();
    bindingParams.addAll(where.parameters());
    return new PreparedQuery(sql, bindingParams);
  }

  /**
   * Builds a no-op UPDATE that re-sets a version column to itself, conditioned on the row still
   * being at the expected version. Used to take this transaction's write lock on a row and detect
   * whether a concurrent writer already moved it past the version read earlier in the same
   * transaction, without changing any of the row's actual values: an updated row count of zero
   * means the row moved.
   *
   * @param tableName Target table.
   * @param versionColumn The version column to re-set to itself; also expected as a key in {@code
   *     whereClause}.
   * @param tableColumns All valid columns of the table, used to validate {@code whereClause}.
   * @param whereClause Conditions identifying the row, including the expected version value.
   * @return UPDATE query with parameter bindings for the WHERE clause only.
   */
  public static PreparedQuery generateVersionCheckUpdateQuery(
      @NonNull String tableName,
      @NonNull String versionColumn,
      @NonNull List<String> tableColumns,
      @NonNull Map<String, Object> whereClause) {
    QueryFragment where = generateWhereClause(new HashSet<>(tableColumns), whereClause, Map.of());
    String sql =
        "UPDATE " + tableName + " SET " + versionColumn + " = " + versionColumn + where.sql();
    return new PreparedQuery(sql, where.parameters());
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
      @NonNull List<String> tableColumns,
      @NonNull String tableName,
      @NonNull Map<String, Object> whereClause) {
    QueryFragment where = generateWhereClause(new HashSet<>(tableColumns), whereClause, Map.of());
    return new PreparedQuery("DELETE FROM " + tableName + where.sql(), where.parameters());
  }

  private static PreparedQuery generateSelectQuery(
      @NonNull List<String> columnNames,
      @NonNull String tableName,
      @NonNull String filter,
      @Nullable String orderByColumn) {
    return generateSelectQuery(columnNames, tableName, filter, orderByColumn, null);
  }

  private static PreparedQuery generateSelectQuery(
      @NonNull List<String> columnNames,
      @NonNull String tableName,
      @NonNull String filter,
      @Nullable String orderByColumn,
      @Nullable Integer limit) {
    if (limit != null && limit <= 0) {
      throw new IllegalArgumentException("Limit must be positive");
    }
    String sql = "SELECT " + String.join(", ", columnNames) + " FROM " + tableName + filter;
    if (orderByColumn != null) {
      sql += " ORDER BY " + orderByColumn + " ASC";
    }
    if (limit != null) {
      sql += " LIMIT " + limit;
    }
    return new PreparedQuery(sql, Collections.emptyList());
  }

  @VisibleForTesting
  static QueryFragment generateWhereClause(
      @NonNull Set<String> tableColumns,
      @NonNull Map<String, Object> whereEquals,
      @NonNull Map<String, Object> whereGreater) {
    return generateWhereClauseExtended(
        tableColumns, whereEquals, whereGreater, Map.of(), Set.of(), Set.of());
  }

  private static void validateColumns(
      @NonNull Set<String> tableColumns, @NonNull Set<String> columns) {
    for (String column : columns) {
      if (!tableColumns.contains(column) && !column.equals("realm_id")) {
        throw new IllegalArgumentException("Invalid query column: " + column);
      }
    }
  }

  @VisibleForTesting
  static QueryFragment generateWhereClauseExtended(
      @NonNull Set<String> tableColumns,
      @NonNull Map<String, Object> whereEquals,
      @NonNull Map<String, Object> whereGreater,
      @NonNull Map<String, Object> whereLess,
      @NonNull Set<String> whereIsNull,
      @NonNull Set<String> whereIsNotNull) {
    validateColumns(tableColumns, whereEquals.keySet());
    validateColumns(tableColumns, whereGreater.keySet());
    validateColumns(tableColumns, whereLess.keySet());
    validateColumns(tableColumns, whereIsNull);
    validateColumns(tableColumns, whereIsNotNull);

    List<String> conditions = new ArrayList<>();
    List<Object> parameters = new ArrayList<>();
    for (Map.Entry<String, Object> entry : whereEquals.entrySet()) {
      conditions.add(entry.getKey() + " = ?");
      parameters.add(entry.getValue());
    }
    for (Map.Entry<String, Object> entry : whereGreater.entrySet()) {
      conditions.add(entry.getKey() + " > ?");
      parameters.add(entry.getValue());
    }
    for (Map.Entry<String, Object> entry : whereLess.entrySet()) {
      conditions.add(entry.getKey() + " < ?");
      parameters.add(entry.getValue());
    }
    for (String column : whereIsNull) {
      conditions.add(column + " IS NULL");
    }
    for (String column : whereIsNotNull) {
      conditions.add(column + " IS NOT NULL");
    }
    String clause = conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions);
    return new QueryFragment(clause, parameters);
  }

  @VisibleForTesting
  static PreparedQuery generateVersionQuery() {
    return new PreparedQuery("SELECT version_value FROM VERSION", List.of());
  }

  /**
   * Generates a {@code SELECT 1 ... WHERE ... LIMIT 1} query to test row existence without fetching
   * any column data. All filter conditions must be supplied in {@code whereClause}.
   */
  public static PreparedQuery generateExistsQuery(
      @NonNull List<String> tableColumns,
      @NonNull String tableName,
      @NonNull Map<String, Object> whereClause) {
    QueryFragment where = generateWhereClause(new HashSet<>(tableColumns), whereClause, Map.of());
    String sql = "SELECT 1 FROM " + tableName + where.sql() + " LIMIT 1";
    return new PreparedQuery(sql, where.parameters());
  }

  @VisibleForTesting
  static PreparedQuery generateEntityTableExistQuery() {
    return new PreparedQuery(
        String.format("SELECT * FROM %s LIMIT 1", ModelEntity.TABLE_NAME), List.of());
  }

  /**
   * Generate a SELECT query to find any entities that have a given realm &amp; parent and that may
   * overlap with a given location. The check is performed without consideration for the scheme, so
   * a path on one storage type may give a false positive for overlapping with another storage type.
   * This should be combined with a check using `StorageLocation`.
   *
   * <p>Equality terms are generated for each prefix of the location in both slash-terminated and
   * non-slash-terminated forms so that ancestors stored with or without a trailing slash are both
   * matched. The lone {@code /} prefix is skipped (not a meaningful storage location); {@code //}
   * and {@code ///} are retained so scheme-root ancestors remain visible to the overlap check.
   *
   * @param realmId A realm to search within
   * @param schemaVersion The schema version of entities table to query
   * @param catalogId A catalog entity to search within
   * @param baseLocation The base location to look for overlap with, with or without a scheme
   * @return The list of possibly overlapping entities that meet the criteria
   */
  @VisibleForTesting
  public static PreparedQuery generateOverlapQuery(
      String realmId, int schemaVersion, long catalogId, String baseLocation) {
    StorageLocation baseStorageLocation = StorageLocation.of(baseLocation);
    String locationWithoutScheme = baseStorageLocation.withoutScheme();

    List<String> conditions = new ArrayList<>();
    List<Object> parameters = new ArrayList<>();

    // Normalize by stripping any trailing slash so the scan below only emits meaningful prefixes.
    // The slash-terminated form is re-added as the last prefix term below.
    String normalizedLocation = locationWithoutScheme;
    if (normalizedLocation.endsWith("/")) {
      normalizedLocation = normalizedLocation.substring(0, normalizedLocation.length() - 1);
    }

    Set<String> prefixTerms = new LinkedHashSet<>();
    for (int i = 0; i < normalizedLocation.length(); i++) {
      if (normalizedLocation.charAt(i) == '/') {
        if (i > 0) {
          prefixTerms.add(normalizedLocation.substring(0, i));
        }
        prefixTerms.add(normalizedLocation.substring(0, i + 1));
      }
    }
    prefixTerms.add(normalizedLocation);
    prefixTerms.add(normalizedLocation + "/");

    for (String prefix : prefixTerms) {
      // Skip only "/", which can never be a meaningful storage location. "//" is kept: in
      // ALLOW_NAMESPACE_CUSTOM_LOCATION mode a namespace location may be a bare scheme root like
      // s3:// (persisted as "//"), and it is a valid ancestor of locations below it. "///" (the
      // root of file: URIs) is kept for the same reason.
      if ("/".equals(prefix)) {
        continue;
      }
      conditions.add("location_without_scheme = ?");
      parameters.add(prefix);
    }

    // Add LIKE condition to match children. Slash-terminate the location so the pattern only
    // matches true descendants (e.g. //bucket/ns/tA/% matches //bucket/ns/tA/child but not
    // //bucket/ns/tA_backup). LIKE wildcards (% and _) and the escape character (\) that appear
    // literally in the location are escaped, with an explicit ESCAPE clause: otherwise a location
    // such as //bucket/my_table/ would be treated as a wildcard, where % and _ over-fetch rows
    // (the caller's precise re-check still filters those) and a literal \ could hide true
    // descendants.
    conditions.add("location_without_scheme LIKE ? ESCAPE '\\'");
    parameters.add(
        escapeLikePattern(StorageLocation.ensureTrailingSlash(locationWithoutScheme)) + "%");

    String locationClause = String.join(" OR ", conditions);
    String clause = " WHERE realm_id = ? AND catalog_id = ? AND (" + locationClause + ")";

    // realmId and parentId go first
    List<Object> finalParams = new ArrayList<>();
    finalParams.add(realmId);
    finalParams.add(catalogId);
    finalParams.addAll(parameters);

    QueryFragment where = new QueryFragment(clause, finalParams);
    PreparedQuery query =
        generateSelectQuery(
            ModelEntity.getAllColumnNames(schemaVersion),
            ModelEntity.TABLE_NAME,
            where.sql(),
            null);
    return new PreparedQuery(query.sql(), where.parameters());
  }

  /**
   * Escapes the LIKE metacharacters {@code %} and {@code _} and the escape character {@code \} in a
   * literal so it can be used safely as a fixed prefix in a {@code LIKE ? ESCAPE '\'} pattern. The
   * backslash is escaped first so the escapes introduced for {@code %} and {@code _} are not
   * double-escaped.
   */
  private static String escapeLikePattern(String literal) {
    return literal.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
  }
}

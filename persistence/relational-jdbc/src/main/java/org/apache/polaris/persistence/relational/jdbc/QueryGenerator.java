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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.persistence.relational.jdbc.models.Converter;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEntity;
import org.apache.polaris.persistence.relational.jdbc.models.ModelGrantRecord;
import org.apache.polaris.persistence.relational.jdbc.models.ModelPolicyMappingRecord;
import org.apache.polaris.persistence.relational.jdbc.models.ModelPrincipalAuthenticationData;

public class QueryGenerator {

  public static class PreparedQuery {
    private final String sql;
    private final List<Object> parameters;

    public PreparedQuery(String sql, List<Object> parameters) {
      this.sql = sql;
      this.parameters = parameters;
    }

    public String getSql() {
      return sql;
    }

    public List<Object> getParameters() {
      return parameters;
    }
  }

  public static <T> PreparedQuery generateSelectQuery(
      @Nonnull Converter<T> entity, @Nonnull Map<String, Object> whereClause) {

    String tableName = getTableName(entity.getClass());
    Map<String, Object> objectMap = entity.toMap();

    String columns = String.join(", ", objectMap.keySet());
    PreparedQuery whereClauseQuery = generateWhereClause(whereClause);
    String sql = "SELECT " + columns + " FROM " + tableName + whereClauseQuery.getSql();

    return new PreparedQuery(sql, whereClauseQuery.getParameters());
  }

  public static PreparedQuery generateDeleteQueryForEntityGrantRecords(
      @Nonnull PolarisEntityCore entity, @Nonnull String realmId) {
    String where =
        " WHERE (grantee_id = ? AND grantee_catalog_id = ? OR securable_id = ? AND securable_catalog_id = ?) AND realm_id = ?";
    List<Object> params =
        Arrays.asList(
            entity.getId(), entity.getCatalogId(), entity.getId(), entity.getCatalogId(), realmId);

    return new PreparedQuery(generateDeleteQuery(ModelGrantRecord.class, where), params);
  }

  public static PreparedQuery generateDeleteQueryForEntityPolicyMappingRecords(
      @Nonnull PolarisBaseEntity entity, @Nonnull String realmId) {
    Map<String, Object> queryParams = new LinkedHashMap<>();
    if (entity.getType() == PolarisEntityType.POLICY) {
      PolicyEntity policyEntity = PolicyEntity.of(entity);
      queryParams.put("policy_type_code", policyEntity.getPolicyTypeCode());
      queryParams.put("policy_catalog_id", policyEntity.getCatalogId());
      queryParams.put("policy_id", policyEntity.getId());
    } else {
      queryParams.put("target_catalog_id", entity.getCatalogId());
      queryParams.put("target_id", entity.getId());
    }
    queryParams.put("realm_id", realmId);

    return generateDeleteQuery(ModelPolicyMappingRecord.class, queryParams);
  }

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
    return new PreparedQuery(generateSelectQuery(new ModelEntity(), where).getSql(), params);
  }

  public static <T> PreparedQuery generateInsertQuery(
      @Nonnull Converter<T> entity, @Nonnull String realmId) {
    String tableName = getTableName(entity.getClass());
    Map<String, Object> obj = entity.toMap();
    List<String> columnNames = new ArrayList<>(obj.keySet());
    List<Object> parameters = new ArrayList<>(obj.values());

    columnNames.add("realm_id");
    parameters.add(realmId);

    String columns = String.join(", ", columnNames);
    String placeholders = columnNames.stream().map(c -> "?").collect(Collectors.joining(", "));

    String sql = "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + placeholders + ")";
    return new PreparedQuery(sql, parameters);
  }

  public static <T> PreparedQuery generateUpdateQuery(
      @Nonnull Converter<T> entity, @Nonnull Map<String, Object> whereClause) {
    String tableName = getTableName(entity.getClass());
    Map<String, Object> obj = entity.toMap();

    List<String> setClauses = new ArrayList<>();
    List<Object> parameters = new ArrayList<>();

    for (Map.Entry<String, Object> entry : obj.entrySet()) {
      setClauses.add(entry.getKey() + " = ?");
      parameters.add(entry.getValue());
    }

    List<String> whereConditions = new ArrayList<>();
    for (Map.Entry<String, Object> entry : whereClause.entrySet()) {
      whereConditions.add(entry.getKey() + " = ?");
      parameters.add(entry.getValue());
    }

    String sql = "UPDATE " + tableName + " SET " + String.join(", ", setClauses);
    if (!whereConditions.isEmpty()) {
      sql += " WHERE " + String.join(" AND ", whereConditions);
    }

    return new PreparedQuery(sql, parameters);
  }

  public static PreparedQuery generateDeleteQuery(
      @Nonnull Class<?> entityClass, @Nonnull Map<String, Object> whereClause) {
    List<String> conditions = new ArrayList<>();
    List<Object> parameters = new ArrayList<>();

    for (Map.Entry<String, Object> entry : whereClause.entrySet()) {
      conditions.add(entry.getKey() + " = ?");
      parameters.add(entry.getValue());
    }

    String where = conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions);
    return new PreparedQuery(generateDeleteQuery(entityClass, where), parameters);
  }

  public static String generateDeleteQuery(
      @Nonnull Class<?> entityClass, @Nonnull String whereClause) {
    return "DELETE FROM " + getTableName(entityClass) + whereClause;
  }

  public static PreparedQuery generateDeleteAll(
      @Nonnull Class<?> entityClass, @Nonnull String realmId) {
    String sql = "DELETE FROM " + getTableName(entityClass) + " WHERE 1 = 1 AND realm_id = ?";
    return new PreparedQuery(sql, List.of(realmId));
  }

  public static <T> PreparedQuery generateDeleteQuery(
      @Nonnull Converter<T> entity, @Nonnull String realmId) {
    String tableName = getTableName(entity.getClass());
    Map<String, Object> objMap = entity.toMap();
    objMap.put("realm_id", realmId);
    return generateDeleteQuery(entity.getClass(), objMap);
  }

  @VisibleForTesting
  static <T> PreparedQuery generateSelectQuery(
      @Nonnull Converter<T> entity, @Nonnull String filter) {
    String tableName = getTableName(entity.getClass());
    Map<String, Object> objectMap = entity.toMap();
    String columns = String.join(", ", objectMap.keySet());
    String sql = "SELECT " + columns + " FROM " + tableName + filter;
    return new PreparedQuery(sql, Collections.emptyList());
  }

  @VisibleForTesting
  static PreparedQuery generateWhereClause(@Nonnull Map<String, Object> whereClause) {
    List<String> conditions = new ArrayList<>();
    List<Object> parameters = new ArrayList<>();

    for (Map.Entry<String, Object> entry : whereClause.entrySet()) {
      conditions.add(entry.getKey() + " = ?");
      parameters.add(entry.getValue());
    }

    String clause = conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions);
    return new PreparedQuery(clause, parameters);
  }

  @VisibleForTesting
  public static String getTableName(@Nonnull Class<?> entityClass) {
    String tableName;
    if (entityClass.equals(ModelEntity.class)) {
      tableName = "ENTITIES";
    } else if (entityClass.equals(ModelGrantRecord.class)) {
      tableName = "GRANT_RECORDS";
    } else if (entityClass.equals(ModelPrincipalAuthenticationData.class)) {
      tableName = "PRINCIPAL_AUTHENTICATION_DATA";
    } else if (entityClass.equals(ModelPolicyMappingRecord.class)) {
      tableName = "POLICY_MAPPING_RECORD";
    } else {
      throw new IllegalArgumentException("Unsupported entity class: " + entityClass.getName());
    }

    return "POLARIS_SCHEMA." + tableName;
  }
}

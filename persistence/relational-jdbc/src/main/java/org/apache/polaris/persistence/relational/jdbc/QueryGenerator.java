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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  public static <T> String generateSelectQuery(
      @Nonnull Converter<T> entity, @Nonnull Map<String, Object> whereClause) {
    return generateSelectQuery(entity, generateWhereClause(whereClause));
  }

  public static String generateDeleteQueryForEntityGrantRecords(
      @Nonnull PolarisEntityCore entity, @Nonnull String realmId) {
    String granteeCondition =
        String.format(
            "grantee_id = %s AND grantee_catalog_id = %s", entity.getId(), entity.getCatalogId());
    String securableCondition =
        String.format(
            "securable_id = %s AND securable_catalog_id = %s",
            entity.getId(), entity.getCatalogId());

    String whereClause =
        " WHERE ("
            + granteeCondition
            + " OR "
            + securableCondition
            + ") AND realm_id = '"
            + realmId
            + "'";
    return generateDeleteQuery(ModelGrantRecord.class, whereClause);
  }

  public static String generateDeleteQueryForEntityPolicyMappingRecords(
      @Nonnull PolarisBaseEntity entity, @Nonnull String realmId) {
    Map<String, Object> queryParams = new HashMap<>();
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

  public static String generateSelectQueryWithEntityIds(
      @Nonnull String realmId, @Nonnull List<PolarisEntityId> entityIds) {
    if (entityIds.isEmpty()) {
      throw new IllegalArgumentException("Empty entity ids");
    }
    StringBuilder condition = new StringBuilder("(catalog_id, id) IN (");
    for (PolarisEntityId entityId : entityIds) {
      String in = "(" + entityId.getCatalogId() + ", " + entityId.getId() + ")";
      condition.append(in);
      condition.append(",");
    }
    // extra , removed
    condition.deleteCharAt(condition.length() - 1);
    condition.append(")");
    condition.append(" AND realm_id = '").append(realmId).append("'");

    return generateSelectQuery(new ModelEntity(), " WHERE " + condition);
  }

  public static <T> String generateInsertQuery(
      @Nonnull Converter<T> entity, @Nonnull String realmId) {
    String tableName = getTableName(entity.getClass());
    Map<String, Object> obj = entity.toMap();
    List<String> columnNames = new ArrayList<>(obj.keySet());
    List<String> values =
        new ArrayList<>(obj.values().stream().map(val -> "'" + val.toString() + "'").toList());
    columnNames.add("realm_id");
    values.add("'" + realmId + "'");

    String columns = String.join(", ", columnNames);
    String valuesString = String.join(", ", values);

    return "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + valuesString + ")";
  }

  public static <T> String generateUpdateQuery(
      @Nonnull Converter<T> entity, @Nonnull Map<String, Object> whereClause) {
    String tableName = getTableName(entity.getClass());
    Map<String, Object> obj = entity.toMap();
    List<String> setClauses = new ArrayList<>();
    List<String> columnNames = new ArrayList<>(obj.keySet());
    List<String> values = obj.values().stream().map(val -> "'" + val.toString() + "'").toList();

    for (int i = 0; i < columnNames.size(); i++) {
      setClauses.add(columnNames.get(i) + " = " + values.get(i)); // Placeholders
    }

    String setClausesString = String.join(", ", setClauses);

    return "UPDATE " + tableName + " SET " + setClausesString + generateWhereClause(whereClause);
  }

  public static String generateDeleteQuery(
      @Nonnull Class<?> entityClass, @Nonnull Map<String, Object> whereClause) {
    return generateDeleteQuery(entityClass, (generateWhereClause(whereClause)));
  }

  public static String generateDeleteQuery(
      @Nonnull Class<?> entityClass, @Nonnull String whereClause) {
    return "DELETE FROM " + getTableName(entityClass) + whereClause;
  }

  public static String generateDeleteAll(@Nonnull Class<?> entityClass, @Nonnull String realmId) {
    String tableName = getTableName(entityClass);
    return "DELETE FROM " + tableName + " WHERE 1 = 1 AND realm_id = '" + realmId + "'";
  }

  public static <T> String generateDeleteQuery(
      @Nonnull Converter<T> entity, @Nonnull String realmId) {
    String tableName = getTableName(entity.getClass());
    Map<String, Object> objMap = entity.toMap();
    objMap.put("realm_id", realmId);
    String whereConditions = generateWhereClause(objMap);
    return "DELETE FROM " + tableName + whereConditions;
  }

  @VisibleForTesting
  public static <T> String generateSelectQuery(
      @Nonnull Converter<T> entity, @Nonnull String filter) {
    String tableName = getTableName(entity.getClass());
    Map<String, Object> objectMap = entity.toMap();
    String columns = String.join(", ", objectMap.keySet());
    StringBuilder query =
        new StringBuilder("SELECT ").append(columns).append(" FROM ").append(tableName);
    if (!filter.isEmpty()) {
      query.append(filter);
    }
    return query.toString();
  }

  @VisibleForTesting
  public static String generateWhereClause(@Nonnull Map<String, Object> whereClause) {
    List<String> whereConditions = new ArrayList<>();

    if (!whereClause.isEmpty()) {
      for (Map.Entry<String, Object> entry : whereClause.entrySet()) {
        String fieldName = entry.getKey();
        Object value = entry.getValue();
        if (value instanceof String) {
          whereConditions.add(fieldName + " = '" + value + "'");
        } else {
          whereConditions.add(fieldName + " = " + value);
        }
      }
    }

    String whereConditionsString = String.join(" AND ", whereConditions);
    return !whereConditionsString.isEmpty() ? (" WHERE " + whereConditionsString) : "";
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

    // TODO: check if we want to make schema name configurable.
    tableName = "POLARIS_SCHEMA." + tableName;

    return tableName;
  }
}

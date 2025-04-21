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
package org.apache.polaris.extension.persistence.relational.jdbc;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.extension.persistence.relational.jdbc.models.ModelEntity;
import org.apache.polaris.extension.persistence.relational.jdbc.models.ModelGrantRecord;
import org.apache.polaris.extension.persistence.relational.jdbc.models.ModelPrincipalAuthenticationData;

public class QueryGenerator {

  private static final Pattern CAMEL_CASE_PATTERN =
      Pattern.compile("(?<=[a-z0-9])[A-Z]|(?<=[A-Z])[A-Z](?=[a-z])");

  public static String generateSelectQuery(
      @Nonnull Class<?> entityClass, @Nonnull Map<String, Object> whereClause) {
    return generateSelectQuery(entityClass, generateWhereClause(whereClause));
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
    return QueryGenerator.generateDeleteQuery(ModelGrantRecord.class, whereClause);
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

    return generateSelectQuery(ModelEntity.class, " WHERE " + condition);
  }

  public static String generateInsertQuery(@Nonnull Object object, @Nonnull String realmId) {
    String tableName = getTableName(object.getClass());

    Class<?> objectClass = object.getClass();
    Field[] fields = objectClass.getDeclaredFields();
    List<String> columnNames = new ArrayList<>();
    List<String> values = new ArrayList<>();
    columnNames.add("realm_id");
    values.add("'" + realmId + "'");

    for (Field field : fields) {
      field.setAccessible(true); // Allow access to private fields
      try {
        Object value = field.get(object);
        if (value != null) { // Only include non-null fields
          columnNames.add(camelToSnake(field.getName()));
          values.add("'" + value + "'");
        }
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    if (columnNames.isEmpty()) {
      throw new RuntimeException("No column names found");
    }

    String columns = String.join(", ", columnNames);
    String valuesString = String.join(", ", values);

    return "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + valuesString + ")";
  }

  public static String generateUpdateQuery(
      @Nonnull Object object, @Nonnull Map<String, Object> whereClause) {
    Class<?> objectClass = object.getClass();
    String tableName = getTableName(objectClass);
    List<String> setClauses = new ArrayList<>();
    Field[] fields = objectClass.getDeclaredFields();
    List<String> columnNames = new ArrayList<>();
    List<Object> values = new ArrayList<>();

    for (Field field : fields) {
      field.setAccessible(true); // Allow access to private fields
      try {
        Object value = field.get(object);
        if (value != null) { // Only include non-null fields
          columnNames.add(camelToSnake(field.getName()));
          values.add("'" + value + "'");
        }
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    for (int i = 0; i < columnNames.size(); i++) {
      setClauses.add(columnNames.get(i) + " = " + values.get(i)); // Placeholders
    }

    String setClausesString = String.join(", ", setClauses);

    return "UPDATE " + tableName + " SET " + setClausesString + generateWhereClause(whereClause);
  }

  public static String generateDeleteQuery(
      @Nonnull Class<?> entityClass, @Nonnull Map<String, Object> whereClause) {
    String tableName = getTableName(entityClass);
    return "DELETE FROM " + tableName + (generateWhereClause(whereClause));
  }

  public static String generateDeleteQuery(
      @Nonnull Class<?> entityClass, @Nonnull String whereClause) {
    return "DELETE FROM " + getTableName(entityClass) + whereClause;
  }

  public static String generateDeleteAll(@Nonnull Class<?> entityClass, @Nonnull String realmId) {
    String tableName = getTableName(entityClass);
    return "DELETE FROM " + tableName + " WHERE 1 = 1 AND realm_id = '" + realmId + "'";
  }

  public static String generateDeleteQuery(
      @Nonnull Object obj, @Nonnull Class<?> entityClass, @Nonnull String realmId) {
    String tableName = getTableName(entityClass);
    List<String> whereConditions = new ArrayList<>();

    Class<?> objectClass = obj.getClass();
    Field[] fields = objectClass.getDeclaredFields();

    for (Field field : fields) {
      field.setAccessible(true); // Allow access to private fields
      try {
        Object value = field.get(obj);
        if (value != null) { // Only include non-null fields
          if (value instanceof String) {
            whereConditions.add(camelToSnake(field.getName()) + " = '" + value + "'");
          } else {
            whereConditions.add(camelToSnake(field.getName()) + " = " + value);
          }
        }
      } catch (IllegalAccessException e) {
        return null; // Or throw an exception
      }
    }

    String whereConditionsString = "";
    if (!whereConditions.isEmpty()) {
      whereConditionsString =
          " WHERE " + String.join(" AND ", whereConditions) + "AND realm_id = '" + realmId + "'";
    }

    return "DELETE FROM " + tableName + whereConditionsString;
  }

  @VisibleForTesting
  public static String generateSelectQuery(@Nonnull Class<?> entityClass, @Nonnull String filter) {
    String tableName = getTableName(entityClass);
    List<String> fields = new ArrayList<>();

    for (Field field : entityClass.getDeclaredFields()) {
      fields.add(camelToSnake(field.getName()));
    }

    String columns = String.join(", ", fields);
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
  public static String camelToSnake(@Nonnull String camelCase) {
    Matcher matcher = CAMEL_CASE_PATTERN.matcher(camelCase);
    StringBuilder sb = new StringBuilder();
    while (matcher.find()) {
      matcher.appendReplacement(sb, "_" + matcher.group(0).toLowerCase(Locale.ROOT));
    }
    matcher.appendTail(sb);
    return sb.toString().toLowerCase(Locale.ROOT);
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
    } else {
      throw new IllegalArgumentException("Unsupported entity class: " + entityClass.getName());
    }

    // TODO: check if we want to make schema name configurable.
    tableName = "POLARIS_SCHEMA." + tableName;

    return tableName;
  }
}

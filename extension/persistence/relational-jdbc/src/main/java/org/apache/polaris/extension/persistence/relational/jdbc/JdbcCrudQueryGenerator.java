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

public class JdbcCrudQueryGenerator {

  private static final Pattern CAMEL_CASE_PATTERN =
      Pattern.compile("(?<=[a-z0-9])[A-Z]|(?<=[A-Z])[A-Z](?=[a-z])");

  public static String generateSelectQuery(
      Class<?> entityClass, String filter, Integer limit, Integer offset, String orderBy) {
    String tableName = getTableName(entityClass);
    List<String> fields = new ArrayList<>();

    for (Field field : entityClass.getDeclaredFields()) {
      fields.add(camelToSnake(field.getName()));
    }

    String columns = String.join(", ", fields);
    StringBuilder query =
        new StringBuilder("SELECT ").append(columns).append(" FROM ").append(tableName);
    if (filter != null && !filter.isEmpty()) {
      query.append(" WHERE ").append(String.join(" AND ", filter));
    }

    return query.toString();
  }

  public static String generateSelectQuery(
      Class<?> entityClass,
      Map<String, Object> whereClause,
      Integer limit,
      Integer offset,
      String orderBy) {
    String tableName = getTableName(entityClass);
    List<String> fields = new ArrayList<>();

    for (Field field : entityClass.getDeclaredFields()) {
      fields.add(camelToSnake(field.getName()));
    }

    String columns = String.join(", ", fields);
    StringBuilder query =
        new StringBuilder("SELECT ").append(columns).append(" FROM ").append(tableName);

    if (whereClause != null && !whereClause.isEmpty()) {
      query.append(generateWhereClause(whereClause));
    }

    if (orderBy != null && !orderBy.isEmpty()) {
      query.append(" ORDER BY ").append(orderBy);
    }

    if (limit != null) {
      query.append(" LIMIT ").append(limit);
    }

    if (offset != null && limit != null) { // Offset only makes sense with limit.
      query.append(" OFFSET ").append(offset);
    }

    return query.toString();
  }

  public static String generateDeleteQueryForEntityGrantRecords(
      PolarisEntityCore entity, String realmId) {
    // generate where clause
    StringBuilder granteeCondition = new StringBuilder("(grantee_id, grantee_catalog_id) IN (");
    granteeCondition
        .append("(")
        .append(entity.getId())
        .append(", ")
        .append(entity.getCatalogId())
        .append(")");
    granteeCondition.append(",");
    // extra , removed
    granteeCondition.deleteCharAt(granteeCondition.length() - 1);
    granteeCondition.append(")");

    StringBuilder securableCondition =
        new StringBuilder("(securable_catalog_id, securable_id) IN (");

    String in = "(" + entity.getCatalogId() + ", " + entity.getId() + ")";
    securableCondition.append(in);
    securableCondition.append(",");

    // extra , removed
    securableCondition.deleteCharAt(securableCondition.length() - 1);
    securableCondition.append(")");

    String whereClause =
        " WHERE ("
            + granteeCondition
            + " OR "
            + securableCondition
            + ") AND realm_id = '"
            + realmId
            + "'";
    return JdbcCrudQueryGenerator.generateDeleteQuery(ModelGrantRecord.class, whereClause);
  }

  public static String generateSelectQueryForMultipleEntities(
      String realmId, List<PolarisEntityId> entityIds) {
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
    return JdbcCrudQueryGenerator.generateSelectQuery(
        ModelEntity.class, entityIds.isEmpty() ? "" : String.valueOf(condition), null, null, null);
  }

  public static String generateInsertQuery(Object object, String realmId) {
    if (object == null) {
      return null;
    }

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
          values.add("'" + value.toString() + "'");
        }
      } catch (IllegalAccessException e) {
        return null; // Or throw an exception
      }
    }

    if (columnNames.isEmpty()) {
      return null; // Or throw an exception if no non-null fields are found
    }

    String columns = String.join(", ", columnNames);
    String valuesString = String.join(", ", values);

    return "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + valuesString + ")";
  }

  public static String generateUpdateQuery(
      Object object, Map<String, Object> whereClause, Class<?> entityClass) {
    String tableName = getTableName(entityClass);
    List<String> setClauses = new ArrayList<>();
    Class<?> objectClass = object.getClass();
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
        // should never happen
      }
    }

    for (int i = 0; i < columnNames.size(); i++) {
      setClauses.add(columnNames.get(i) + " = " + values.get(i)); // Placeholders
    }

    String setClausesString = String.join(", ", setClauses);

    return "UPDATE " + tableName + " SET " + setClausesString + generateWhereClause(whereClause);
  }

  public static String generateDeleteQuery(Map<String, Object> whereClause, Class<?> entityClass) {
    String tableName = getTableName(entityClass);
    return "DELETE FROM " + tableName + (generateWhereClause(whereClause));
  }

  public static String generateDeleteQuery(Class<?> entityClass, String whereClause) {
    return "DELETE FROM " + getTableName(entityClass) + whereClause;
  }

  public static String generateDeleteAll(Class<?> entityClass, String realmId) {
    String tableName = getTableName(entityClass);
    return "DELETE FROM " + tableName + " WHERE 1 = 1 AND realm_id = '" + realmId + "'";
  }

  public static String generateDeleteQuery(Object obj, Class<?> entityClass, String realmId) {
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

  private static String generateWhereClause(Map<String, Object> whereClause) {
    List<String> whereConditions = new ArrayList<>();

    if (whereClause != null && !whereClause.isEmpty()) {
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

  private static String camelToSnake(String camelCase) {
    Matcher matcher = CAMEL_CASE_PATTERN.matcher(camelCase);
    StringBuilder sb = new StringBuilder();
    while (matcher.find()) {
      matcher.appendReplacement(sb, "_" + matcher.group(0).toLowerCase(Locale.ROOT));
    }
    matcher.appendTail(sb);
    return sb.toString().toLowerCase(Locale.ROOT);
  }

  private static String getTableName(Class<?> entityClass) {
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

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

import jakarta.annotation.Nonnull;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Predicate;

public class ResultSetToObjectConverter {

  public static <T, R> List<R> collect(
      @Nonnull ResultSet resultSet,
      @Nonnull Class<T> targetClass,
      @Nonnull Function<T, R> transformer,
      Predicate<R> entityFilter,
      int limit)
      throws ReflectiveOperationException, SQLException {
    List<R> resultList = new ArrayList<>();
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();
    String[] columnNames = new String[columnCount + 1]; // 1-based indexing

    for (int i = 1; i <= columnCount; i++) {
      columnNames[i] =
          metaData
              .getColumnLabel(i)
              .toLowerCase(Locale.ROOT); // or getColumnName(), lowercase to match field names
    }

    while (resultSet.next() && resultList.size() < limit) {
      T object = targetClass.getDeclaredConstructor().newInstance(); // Create a new instance
      for (int i = 1; i <= columnCount; i++) {
        String columnName = columnNames[i];
        Object value;
        // TODO: This handling doesn't works for H2, works fine with Postgres.
        //        if (columnName.contains("properties")) {
        //          value = resultSet.getString(i);
        //        } else {
        //          value = resultSet.getObject(i);
        //        }
        value = resultSet.getObject(i);

        try {
          Field field = targetClass.getDeclaredField(columnName);
          field.setAccessible(true); // Allow access to private fields
          field.set(object, value);
        } catch (NoSuchFieldException e) {
          // Handle case where column name doesn't match field name (e.g., snake_case vs. camelCase)
          // You could implement more sophisticated matching logic here, or use annotations.
          try {
            Field field = targetClass.getDeclaredField(convertSnakeCaseToCamelCase(columnName));
            field.setAccessible(true);
            field.set(object, value);
          } catch (NoSuchFieldException e2) {
            // if still not found, just skip it.
          }
        }
      }
      R entity = transformer.apply(object);
      if (entityFilter == null || entityFilter.test(entity)) {
        resultList.add(entity);
      }
    }
    return resultList;
  }

  public static String convertSnakeCaseToCamelCase(String snakeCase) {
    StringBuilder camelCase = new StringBuilder();
    boolean nextUpperCase = false;
    for (char c : snakeCase.toCharArray()) {
      if (c == '_') {
        nextUpperCase = true;
      } else {
        if (nextUpperCase) {
          camelCase.append(Character.toUpperCase(c));
          nextUpperCase = false;
        } else {
          camelCase.append(c);
        }
      }
    }
    return camelCase.toString();
  }
}

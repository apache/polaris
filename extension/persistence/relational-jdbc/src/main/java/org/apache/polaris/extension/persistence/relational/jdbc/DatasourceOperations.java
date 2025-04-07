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

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import javax.sql.DataSource;

import static java.nio.charset.StandardCharsets.UTF_8;

public class DatasourceOperations {
  DataSource datasource;

  public DatasourceOperations(DataSource datasource) {
    this.datasource = datasource;
  }

  public void executeScript() {
    System.out.println("Executing script");
    ClassLoader classLoader = DatasourceOperations.class.getClassLoader();
    try (Connection connection = borrowConnection();
         Statement statement = connection.createStatement()) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream("postgres/schema-v1-postgres.sql"), UTF_8));
      StringBuilder sqlBuffer = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (!line.isEmpty() && !line.startsWith("--")) { // Ignore empty lines and comments
          sqlBuffer.append(line).append("\n");
          if (line.endsWith(";")) { // Execute statement when semicolon is found
            String sql = sqlBuffer.toString().trim();
            System.out.println("SQL printing " + sql);
            try {
              boolean hasResults = statement.execute(sql);
              if (hasResults) {
                // Process ResultSet if needed (for SELECT queries)
                System.out.println("Query executed and returned results.");
                // You would typically fetch and process the ResultSet here
              } else {
                int updateCount = statement.getUpdateCount();
                System.out.println("Query executed, " + updateCount + " rows affected.");
              }
            } catch (SQLException e) {
              System.err.println("Error executing SQL: " + sql);
              e.printStackTrace();
            }
            sqlBuffer.setLength(0); // Clear the buffer for the next statement
          }
        }
      }

      sqlBuffer.append("SHOW TABLES FROM polaris_schema");
      ResultSet resultSet = statement.executeQuery(sqlBuffer.toString());
      System.out.println("Query executed and returned results.");
      while (resultSet.next()) {
        System.out.println(resultSet.getString("TABLE_NAME"));
      }
      System.out.println("SQL for tables done");
      sqlBuffer.setLength(0);

    } catch (IOException e) {
      e.printStackTrace();
    } catch (SQLException e) {
        throw new RuntimeException(e);
    }
  }

  public <T> List<T> executeSelect(String query, Class<T> targetClass) {
    System.out.println("Executing query select query: " + query);
    try (Connection connection = borrowConnection();
        Statement statement = connection.createStatement()) {
      ResultSet s = statement.executeQuery(query);
      List<T> x = ResultSetToObjectConverter.convert(s, targetClass);
      return x == null || x.isEmpty() ? null : x;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
    return null;
  }

  public int executeUpdate(String query) {
    System.out.println("Executing query: " + query);
    try (Connection connection = borrowConnection();
        Statement statement = connection.createStatement()) {
      ResultSet s = statement.executeQuery("SHOW TABLES from polaris_schema");
      s.next();
      return statement.executeUpdate(query);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return 0;
  }

  public int executeUpdate(String query, Statement statement) throws SQLException {
    System.out.println("Executing query in transaction : " + query);
    int i = 0;
    try {
      i = statement.executeUpdate(query);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    System.out.println("Query executed: " + i);
    return i;
  }

  public boolean runWithinTransaction(TransactionCallback callback) throws Exception {
    System.out.println("Executing transaction within callback: " + callback);
    Connection connection = null;
    try {
      connection = borrowConnection();
      connection.setAutoCommit(false); // Disable auto-commit to start a transaction

      boolean result = false;
      try (Statement statement = connection.createStatement()) {
        result = callback.execute(statement);
      }

      if (result) {
        connection.commit(); // Commit the transaction if successful
        return true;
      } else {
        connection.rollback(); // Rollback the transaction if not successful
        return false;
      }

    } catch (Exception e) {
      if (connection != null) {
        try {
          connection.rollback(); // Rollback on exception
        } catch (SQLException ex) {
          ex.printStackTrace(); // Log rollback exception
        }
      }
      e.printStackTrace();
      throw e;
    } finally {
      if (connection != null) {
        try {
          connection.setAutoCommit(true); // Restore auto-commit
          connection.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  // Interface for transaction callback
  public interface TransactionCallback {
    boolean execute(Statement statement) throws SQLException;
  }

  Connection borrowConnection() throws SQLException {
    Connection c = datasource.getConnection();
    return c;
  }
}

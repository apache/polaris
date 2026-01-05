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

import java.io.InputStream;
import java.util.Locale;

/**
 * Database types supported by Polaris relational JDBC persistence.
 *
 * <p>CockroachDB uses the PostgreSQL wire protocol but has its own schema resources in the
 * "cockroachdb" directory. While CockroachDB is PostgreSQL-compatible, having separate schemas
 * allows for CockroachDB-specific optimizations and avoids experimental ALTER operations that
 * CockroachDB doesn't fully support.
 */
public enum DatabaseType {
  POSTGRES("postgres"),
  COCKROACHDB("cockroachdb"),
  H2("h2");

  private final String displayName; // Store the user-friendly name

  DatabaseType(String displayName) {
    this.displayName = displayName;
  }

  // Method to get the user-friendly display name
  public String getDisplayName() {
    return displayName;
  }

  /**
   * Returns the latest schema version available for this database type. This is used as the default
   * schema version for new installations.
   */
  public int getLatestSchemaVersion() {
    return switch (this) {
      case POSTGRES -> 3; // PostgreSQL has schemas v1, v2, v3
      case COCKROACHDB -> 1; // CockroachDB currently has only schema v1
      case H2 -> 3; // H2 uses same schemas as PostgreSQL
    };
  }

  public static DatabaseType fromDisplayName(String displayName) {
    return switch (displayName.toLowerCase(Locale.ROOT)) {
      case "h2" -> DatabaseType.H2;
      case "postgresql" -> DatabaseType.POSTGRES;
      case "cockroachdb" -> DatabaseType.COCKROACHDB;
      default -> throw new IllegalStateException("Unsupported DatabaseType: '" + displayName + "'");
    };
  }

  /**
   * Determines the database type from JDBC connection metadata and validates against configured
   * type.
   *
   * <p>Logic:
   *
   * <ul>
   *   <li>If configuredType is provided: uses it as the authoritative type and validates it matches
   *       what the connection reports. Throws an exception if there's a mismatch.
   *   <li>If configuredType is null: infers the type from connection metadata.
   * </ul>
   *
   * @param connection JDBC connection to inspect
   * @param configuredType Explicitly configured database type (may be null)
   * @return The database type (configured if provided, otherwise inferred)
   * @throws IllegalStateException if configured type doesn't match connection metadata
   */
  public static DatabaseType inferFromConnection(
      java.sql.Connection connection, DatabaseType configuredType) {
    try {
      var metaData = connection.getMetaData();
      String productName = metaData.getDatabaseProductName().toLowerCase(Locale.ROOT);

      // Infer database type from connection metadata
      DatabaseType inferredType = null;

      if (productName.contains("cockroach")) {
        inferredType = DatabaseType.COCKROACHDB;
      } else if (productName.contains("postgresql")) {
        inferredType = DatabaseType.POSTGRES;
      } else if (productName.contains("h2")) {
        inferredType = DatabaseType.H2;
      }

      // If a type was explicitly configured, use it and validate
      if (configuredType != null) {
        if (inferredType != null && inferredType != configuredType) {
          // Special case: CockroachDB uses PostgreSQL JDBC driver and wire protocol
          // So configured=COCKROACHDB with inferred=POSTGRES is valid
          boolean isValidCockroachConfig =
              configuredType == DatabaseType.COCKROACHDB && inferredType == DatabaseType.POSTGRES;

          if (!isValidCockroachConfig) {
            throw new IllegalStateException(
                String.format(
                    "Configured database type '%s' does not match detected type '%s' from connection (product: '%s'). "
                        + "Please check your polaris.persistence.relational.jdbc.database-type configuration.",
                    configuredType, inferredType, productName));
          }
        }
        // Use configured type (either it matches, it's valid CockroachDB, or we couldn't infer)
        return configuredType;
      }

      // No configured type - use inferred type
      if (inferredType != null) {
        return inferredType;
      }

      // Couldn't infer and no configured type
      throw new IllegalStateException(
          "Cannot infer database type from product name: '"
              + productName
              + "'. "
              + "Please set polaris.persistence.relational.jdbc.database-type explicitly.");

    } catch (java.sql.SQLException e) {
      // If we can't get metadata, must have a configured type
      if (configuredType != null) {
        return configuredType;
      }
      throw new IllegalStateException(
          "Cannot determine database type: unable to read connection metadata", e);
    }
  }

  /**
   * Open an InputStream that contains data from an init script. This stream should be closed by the
   * caller.
   */
  public InputStream openInitScriptResource(int schemaVersion) {
    // Validate schema version is within acceptable range for this database type
    int latestVersion = getLatestSchemaVersion();
    if (schemaVersion <= 0 || schemaVersion > latestVersion) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid schema version %d for database type %s. Valid range: 1-%d",
              schemaVersion, this, latestVersion));
    }

    final String resourceName =
        String.format("%s/schema-v%d.sql", this.getDisplayName(), schemaVersion);

    ClassLoader classLoader = DatasourceOperations.class.getClassLoader();
    InputStream stream = classLoader.getResourceAsStream(resourceName);
    if (stream == null) {
      throw new IllegalStateException(
          String.format(
              "Schema resource not found: %s (database type: %s, version: %d)",
              resourceName, this, schemaVersion));
    }
    return stream;
  }

  /**
   * Open an InputStream that contains data from the metrics schema init script. This stream should
   * be closed by the caller.
   *
   * @param metricsSchemaVersion the metrics schema version (currently only 1 is supported)
   * @return an InputStream for the metrics schema SQL file
   */
  public InputStream openMetricsSchemaResource(int metricsSchemaVersion) {
    if (metricsSchemaVersion != 1) {
      throw new IllegalArgumentException(
          "Unknown or invalid metrics schema version " + metricsSchemaVersion);
    }

    final String resourceName =
        String.format("%s/schema-metrics-v%d.sql", this.getDisplayName(), metricsSchemaVersion);

    ClassLoader classLoader = DatasourceOperations.class.getClassLoader();
    return classLoader.getResourceAsStream(resourceName);
  }
}

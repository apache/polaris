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

import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.apache.polaris.test.commons.PostgresRelationalJdbcLifeCycleManagement;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Runs {@link org.apache.polaris.core.persistence.BasePolarisMetaStoreManagerTest} integration
 * tests against every PostgreSQL schema version on the classpath.
 */
@Testcontainers
@ParameterizedClass
@MethodSource("schemaVersions")
public class AtomicMetastoreManagerWithJdbcBasePersistenceImplPostgresSchemaIT
    extends AtomicMetastoreManagerWithJdbcBasePersistenceImplTest {

  @Container
  private static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer(
          containerSpecHelper("postgres", PostgresRelationalJdbcLifeCycleManagement.class)
              .dockerImageName(null)
              .asCompatibleSubstituteFor("postgres"));

  @Parameter int schemaVersion;

  private DataSource dataSource;

  static Stream<Integer> schemaVersions() {
    return SchemaVersions.discoverAsStream(DatabaseType.POSTGRES);
  }

  @Override
  protected DatabaseType databaseType() {
    return DatabaseType.POSTGRES;
  }

  @Override
  public int schemaVersion() {
    return schemaVersion;
  }

  @Override
  protected DataSource createDataSource() {
    if (dataSource == null) {
      dataSource = createPostgresDataSource(schemaVersion());
    }
    return dataSource;
  }

  private static DataSource createPostgresDataSource(int schemaVersion) {
    String databaseName = "polaris_schema_v" + schemaVersion;
    createDatabaseIfNotExists(databaseName);
    createPolarisSchema(databaseName);

    PGSimpleDataSource postgresDataSource = new PGSimpleDataSource();
    postgresDataSource.setURL(jdbcUrlForDatabase(databaseName));
    postgresDataSource.setUser(POSTGRES.getUsername());
    postgresDataSource.setPassword(POSTGRES.getPassword());
    // The schema is provided by the datasource, not by the persistence code (the currentSchema
    // driver setting in a real deployment).
    postgresDataSource.setCurrentSchema("polaris_schema");
    return postgresDataSource;
  }

  /** The DBA step in a real deployment: the schema must exist before Polaris connects. */
  private static void createPolarisSchema(String databaseName) {
    try (Connection connection =
            DriverManager.getConnection(
                jdbcUrlForDatabase(databaseName), POSTGRES.getUsername(), POSTGRES.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA IF NOT EXISTS polaris_schema");
    } catch (SQLException e) {
      throw new RuntimeException("Failed to create schema in " + databaseName, e);
    }
  }

  private static void createDatabaseIfNotExists(String databaseName) {
    try (Connection connection =
            DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + databaseName);
    } catch (SQLException e) {
      // 42P04 is PostgreSQL's duplicate_database error; ignore when the schema DB already exists.
      if (!"42P04".equals(e.getSQLState())) {
        throw new RuntimeException("Failed to create database " + databaseName, e);
      }
    }
  }

  private static String jdbcUrlForDatabase(String databaseName) {
    String baseUrl = POSTGRES.getJdbcUrl();
    int lastSlash = baseUrl.lastIndexOf('/');
    if (lastSlash < 0) {
      throw new IllegalStateException("Unexpected JDBC URL: " + baseUrl);
    }
    return baseUrl.substring(0, lastSlash + 1) + databaseName;
  }
}

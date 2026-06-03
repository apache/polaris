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
import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.test.commons.PostgresRelationalJdbcLifeCycleManagement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * PostgreSQL integration coverage for grant-record insert idempotency. Complements {@link
 * JdbcGrantRecordsIdempotencyTest}, which exercises H2 across schema versions v1–v4.
 */
@Testcontainers
class JdbcGrantRecordsIdempotencyPostgresIT {

  private static final RealmContext REALM_CONTEXT = () -> "REALM";
  private static final int SCHEMA_VERSION = 4;

  private static final long SECURABLE_CATALOG_ID = 1L;
  private static final long SECURABLE_ID = 2L;
  private static final long GRANTEE_CATALOG_ID = 3L;
  private static final long GRANTEE_ID = 4L;
  private static final int PRIVILEGE_CODE = 21;

  @Container
  private static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer(
          containerSpecHelper("postgres", PostgresRelationalJdbcLifeCycleManagement.class)
              .dockerImageName(null)
              .asCompatibleSubstituteFor("postgres"));

  private static PGSimpleDataSource dataSource;
  private static JdbcBasePersistenceImpl basePersistence;

  @BeforeAll
  static void setUp() throws Exception {
    POSTGRES.start();
    dataSource = new PGSimpleDataSource();
    dataSource.setURL(POSTGRES.getJdbcUrl());
    dataSource.setUser(POSTGRES.getUsername());
    dataSource.setPassword(POSTGRES.getPassword());

    RelationalJdbcConfiguration configuration =
        new RelationalJdbcConfiguration() {
          @Override
          public Optional<Integer> maxRetries() {
            return Optional.of(2);
          }

          @Override
          public Optional<Long> maxDurationInMs() {
            return Optional.of(100L);
          }

          @Override
          public Optional<Long> initialDelayInMs() {
            return Optional.of(100L);
          }

          @Override
          public Optional<String> databaseType() {
            return Optional.empty();
          }
        };

    DatasourceOperations datasourceOperations = new DatasourceOperations(dataSource, configuration);
    try (InputStream scriptStream =
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("postgres/schema-v4.sql")) {
      if (scriptStream == null) {
        throw new IllegalStateException("postgres/schema-v4.sql not found on classpath");
      }
      datasourceOperations.executeScript(scriptStream);
    }

    basePersistence =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            datasourceOperations,
            RANDOM_SECRETS,
            mock(PolarisStorageIntegrationProvider.class),
            REALM_CONTEXT.getRealmIdentifier(),
            SCHEMA_VERSION);
  }

  @AfterAll
  static void tearDown() {
    POSTGRES.stop();
  }

  @Test
  void writeToGrantRecordsIsIdempotentOnPostgres() throws SQLException {
    PolarisCallContext callCtx = new PolarisCallContext(REALM_CONTEXT, basePersistence);
    PolarisGrantRecord grant =
        new PolarisGrantRecord(
            SECURABLE_CATALOG_ID, SECURABLE_ID, GRANTEE_CATALOG_ID, GRANTEE_ID, PRIVILEGE_CODE);

    assertThatCode(() -> basePersistence.writeToGrantRecords(callCtx, grant))
        .doesNotThrowAnyException();
    assertThatCode(() -> basePersistence.writeToGrantRecords(callCtx, grant))
        .doesNotThrowAnyException();

    assertThat(countMatchingGrantRows()).isEqualTo(1);
  }

  private static int countMatchingGrantRows() throws SQLException {
    String sql =
        "SELECT COUNT(*) FROM polaris_schema.grant_records "
            + "WHERE realm_id = ? AND securable_catalog_id = ? AND securable_id = ? "
            + "AND grantee_catalog_id = ? AND grantee_id = ? AND privilege_code = ?";
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, REALM_CONTEXT.getRealmIdentifier());
      statement.setLong(2, SECURABLE_CATALOG_ID);
      statement.setLong(3, SECURABLE_ID);
      statement.setLong(4, GRANTEE_CATALOG_ID);
      statement.setLong(5, GRANTEE_ID);
      statement.setInt(6, PRIVILEGE_CODE);
      try (ResultSet resultSet = statement.executeQuery()) {
        resultSet.next();
        return resultSet.getInt(1);
      }
    }
  }
}

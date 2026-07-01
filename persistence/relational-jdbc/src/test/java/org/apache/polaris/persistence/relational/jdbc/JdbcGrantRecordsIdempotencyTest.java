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

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Optional;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class JdbcGrantRecordsIdempotencyTest {

  private static final RealmContext REALM_CONTEXT = () -> "REALM";

  private static final long SECURABLE_CATALOG_ID = 1L;
  private static final long SECURABLE_ID = 2L;
  private static final long GRANTEE_CATALOG_ID = 3L;
  private static final long GRANTEE_ID = 4L;
  private static final int PRIVILEGE_CODE = 21;

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4})
  void writeToGrantRecordsIsIdempotent(int schemaVersion) throws SQLException {
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:grant_idempotency_v"
                + schemaVersion
                + "_"
                + System.nanoTime()
                + ";DB_CLOSE_DELAY=-1",
            "sa",
            "");
    DatasourceOperations datasourceOperations =
        new DatasourceOperations(dataSource, new TestJdbcConfiguration());
    try (InputStream scriptStream = DatabaseType.H2.openInitScriptResource(schemaVersion)) {
      datasourceOperations.executeScript(scriptStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    JdbcBasePersistenceImpl basePersistence =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            datasourceOperations,
            RANDOM_SECRETS,
            REALM_CONTEXT.getRealmIdentifier(),
            schemaVersion);
    PolarisCallContext callCtx = new PolarisCallContext(REALM_CONTEXT, basePersistence);

    PolarisGrantRecord grant =
        new PolarisGrantRecord(
            SECURABLE_CATALOG_ID, SECURABLE_ID, GRANTEE_CATALOG_ID, GRANTEE_ID, PRIVILEGE_CODE);

    assertThatCode(() -> basePersistence.writeToGrantRecords(callCtx, grant))
        .doesNotThrowAnyException();
    assertThatCode(() -> basePersistence.writeToGrantRecords(callCtx, grant))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "23502", // NOT NULL violation
        "23503", // FK violation
        "23513", // CHECK violation
        "08006", // connection failure
        "40001" // serialization failure
      })
  void writeToGrantRecords_IdempotencyCheck_OnlyCatchesPKUniqueness(String sqlStateCode)
      throws SQLException {
    SQLException nonUniqueViolation = new SQLException(sqlStateCode, sqlStateCode);

    DatasourceOperations datasourceOperations = Mockito.mock(DatasourceOperations.class);
    when(datasourceOperations.getDatabaseType()).thenReturn(DatabaseType.H2);
    when(datasourceOperations.getQueryGenerator())
        .thenReturn(new QueryGenerator(QueryGenerator.DEFAULT_SCHEMA_NAME));
    doThrow(nonUniqueViolation)
        .when(datasourceOperations)
        .executeUpdate(any(QueryGenerator.PreparedQuery.class));
    doCallRealMethod()
        .when(datasourceOperations)
        .isUniquenessConstraintViolation(any(SQLException.class));

    int schemaVersion = 4;
    JdbcBasePersistenceImpl basePersistence =
        new JdbcBasePersistenceImpl(
            new PolarisDefaultDiagServiceImpl(),
            datasourceOperations,
            RANDOM_SECRETS,
            REALM_CONTEXT.getRealmIdentifier(),
            schemaVersion);
    PolarisCallContext callCtx = new PolarisCallContext(REALM_CONTEXT, basePersistence);

    PolarisGrantRecord grant =
        new PolarisGrantRecord(
            SECURABLE_CATALOG_ID, SECURABLE_ID, GRANTEE_CATALOG_ID, GRANTEE_ID, PRIVILEGE_CODE);

    assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> basePersistence.writeToGrantRecords(callCtx, grant))
        .withMessageContaining("Failed to write to grant records")
        .withCause(nonUniqueViolation);
  }

  private static final class TestJdbcConfiguration implements RelationalJdbcConfiguration {
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
      return Optional.of("h2");
    }

    @Override
    public Optional<String> schemaName() {
      return Optional.empty();
    }
  }
}

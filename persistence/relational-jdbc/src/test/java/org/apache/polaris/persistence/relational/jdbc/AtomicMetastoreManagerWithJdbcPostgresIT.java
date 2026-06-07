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

import java.util.Optional;
import javax.sql.DataSource;
import org.apache.polaris.test.commons.PostgresRelationalJdbcLifeCycleManagement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * PostgreSQL integration coverage for {@link
 * org.apache.polaris.core.persistence.BasePolarisMetaStoreManagerTest#testGrantRecordWriteIsIdempotent()}.
 * Complements {@link JdbcGrantRecordsIdempotencyTest}, which exercises the JDBC insert path on H2
 * across schema versions v1–v4.
 */
@Testcontainers
public class AtomicMetastoreManagerWithJdbcPostgresIT {

  @Container
  private static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer(
          containerSpecHelper("postgres", PostgresRelationalJdbcLifeCycleManagement.class)
              .dockerImageName(null)
              .asCompatibleSubstituteFor("postgres"));

  private static PostgresMetaStoreManagerHarness harness;

  @BeforeAll
  static void startPostgres() {
    POSTGRES.start();
    PGSimpleDataSource dataSource = new PGSimpleDataSource();
    dataSource.setURL(POSTGRES.getJdbcUrl());
    dataSource.setUser(POSTGRES.getUsername());
    dataSource.setPassword(POSTGRES.getPassword());
    harness = new PostgresMetaStoreManagerHarness(dataSource);
  }

  @AfterAll
  static void stopPostgres() {
    POSTGRES.stop();
  }

  @Test
  void testGrantRecordWriteIsIdempotentOnPostgres() {
    harness.runGrantRecordWriteIsIdempotentTest();
  }

  private static final class PostgresMetaStoreManagerHarness
      extends AtomicMetastoreManagerWithJdbcBasePersistenceImplTest {

    private final DataSource dataSource;

    private PostgresMetaStoreManagerHarness(DataSource dataSource) {
      this.dataSource = dataSource;
    }

    void runGrantRecordWriteIsIdempotentTest() {
      setupPolarisMetaStoreManager();
      testGrantRecordWriteIsIdempotent();
    }

    @Override
    public int schemaVersion() {
      return 4;
    }

    @Override
    protected DataSource createDataSource() {
      return dataSource;
    }

    @Override
    protected DatabaseType databaseType() {
      return DatabaseType.POSTGRES;
    }

    @Override
    protected RelationalJdbcConfiguration createJdbcConfiguration() {
      return new PostgresJdbcConfiguration();
    }
  }

  private static final class PostgresJdbcConfiguration implements RelationalJdbcConfiguration {
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
  }
}

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import jakarta.enterprise.inject.Instance;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JdbcMetaStoreManagerFactoryTest {

  @Mock private Instance<DataSource> dataSourceInstance;

  @Test
  void produceDatasourceOperationsUsesQuarkusDataSourceWhenJdbcUrlIsAbsent() throws Exception {
    DataSource dataSource = mockDataSource("PostgreSQL");
    when(dataSourceInstance.get()).thenReturn(dataSource);

    DatasourceOperations operations =
        JdbcMetaStoreManagerFactory.produceDatasourceOperations(
            dataSourceInstance, TestingRelationalJdbcConfiguration.builder().build());

    try {
      assertThat(operations.ownsDataSource()).isFalse();
      assertThat(operations.getDatabaseType()).isEqualTo(DatabaseType.POSTGRES);
      verify(dataSourceInstance).get();
    } finally {
      operations.close();
    }
  }

  @Test
  void produceDatasourceOperationsUsesPolarisDataSourceWhenJdbcUrlIsPresent() {
    RelationalJdbcConfiguration configuration =
        TestingRelationalJdbcConfiguration.builder()
            .jdbcUrl("jdbc:h2:mem:producer_poc;DB_CLOSE_DELAY=-1")
            .driver("org.h2.Driver")
            .username("sa")
            .password("")
            .build();

    DatasourceOperations operations =
        JdbcMetaStoreManagerFactory.produceDatasourceOperations(dataSourceInstance, configuration);

    try {
      assertThat(operations.ownsDataSource()).isTrue();
      assertThat(operations.getDatabaseType()).isEqualTo(DatabaseType.H2);
      verify(dataSourceInstance, never()).get();
    } finally {
      operations.close();
    }
  }

  @Test
  void closeDatasourceOperationsClosesOnlyOwnedDataSources() throws Exception {
    DataSource ownedDataSource = closeableMockDataSource("PostgreSQL");
    DatasourceOperations ownedOperations =
        new DatasourceOperations(
            ownedDataSource, TestingRelationalJdbcConfiguration.builder().build(), true);

    ownedOperations.close();

    verify((AutoCloseable) ownedDataSource).close();

    DataSource externalDataSource = closeableMockDataSource("PostgreSQL");
    DatasourceOperations externalOperations =
        new DatasourceOperations(
            externalDataSource, TestingRelationalJdbcConfiguration.builder().build(), false);

    externalOperations.close();

    verify((AutoCloseable) externalDataSource, never()).close();
  }

  private static DataSource mockDataSource(String databaseProductName) throws Exception {
    DataSource dataSource = mock(DataSource.class);
    stubConnection(dataSource, databaseProductName);
    return dataSource;
  }

  private static DataSource closeableMockDataSource(String databaseProductName) throws Exception {
    DataSource dataSource =
        mock(DataSource.class, withSettings().extraInterfaces(AutoCloseable.class));
    stubConnection(dataSource, databaseProductName);
    return dataSource;
  }

  private static void stubConnection(DataSource dataSource, String databaseProductName)
      throws Exception {
    Connection connection = mock(Connection.class);
    DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(databaseMetaData);
    when(databaseMetaData.getDatabaseProductName()).thenReturn(databaseProductName);
  }
}

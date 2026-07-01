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
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RelationalJdbcProductionReadinessChecksTest {

  private RelationalJdbcProductionReadinessChecks checks;

  @Mock private Instance<DatasourceOperations> datasourceOperations;

  @BeforeEach
  void setUp() {
    checks = new RelationalJdbcProductionReadinessChecks();
  }

  @Test
  void nonJdbcMetaStoreManagerFactoryReturnsOk() {
    MetaStoreManagerFactory metaStoreManagerFactory = mock(MetaStoreManagerFactory.class);

    ProductionReadinessCheck result =
        checks.checkRelationalJdbc(metaStoreManagerFactory, datasourceOperations);

    assertThat(result.ready()).isTrue();
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  void jdbcWithH2ReturnsWarning() {
    JdbcMetaStoreManagerFactory metaStoreManagerFactory = mock(JdbcMetaStoreManagerFactory.class);
    DatasourceOperations operations = mock(DatasourceOperations.class);
    when(datasourceOperations.get()).thenReturn(operations);
    when(operations.getDatabaseType()).thenReturn(DatabaseType.H2);

    ProductionReadinessCheck result =
        checks.checkRelationalJdbc(metaStoreManagerFactory, datasourceOperations);

    assertThat(result.ready()).isFalse();
    assertThat(result.getErrors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.message())
                  .isEqualTo("The current persistence (jdbc:h2) is intended for tests only.");
              assertThat(error.offendingProperty())
                  .isEqualTo("polaris.persistence.relational.jdbc.datasource");
              assertThat(error.severe()).isFalse();
            });
  }

  @Test
  void jdbcWithPostgresReturnsOk() {
    JdbcMetaStoreManagerFactory metaStoreManagerFactory = mock(JdbcMetaStoreManagerFactory.class);
    DatasourceOperations operations = mock(DatasourceOperations.class);
    when(datasourceOperations.get()).thenReturn(operations);
    when(operations.getDatabaseType()).thenReturn(DatabaseType.POSTGRES);

    ProductionReadinessCheck result =
        checks.checkRelationalJdbc(metaStoreManagerFactory, datasourceOperations);

    assertThat(result.ready()).isTrue();
    assertThat(result.getErrors()).isEmpty();
  }
}

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
package org.apache.polaris.persistence.relational.jdbc.mysql.tests;

import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mysql.cj.jdbc.MysqlDataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.QueryGenerator;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.apache.polaris.persistence.relational.jdbc.models.ModelPolicyMappingRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * End-to-end coverage for the JSON placeholder selection described on {@code ModelRegistry}: fails
 * with 0 rows deleted if {@link DatabaseType#asJsonConditionPlaceholder()} stops emitting {@code
 * CAST(? AS JSON)}, which no unit test can catch because they only inspect generated SQL.
 */
@Testcontainers
public class MysqlJsonColumnPredicateIT {

  private static final String REALM_ID = "POLARIS";

  record TestConfiguration(
      Optional<Integer> maxRetries,
      Optional<Long> maxDurationInMs,
      Optional<Long> initialDelayInMs,
      Optional<String> databaseType)
      implements RelationalJdbcConfiguration {}

  @Container
  @SuppressWarnings("resource")
  private static final MySQLContainer<?> MYSQL =
      new MySQLContainer<>(
              containerSpecHelper("mysql", MysqlJsonColumnPredicateIT.class)
                  .dockerImageName(null)
                  .asCompatibleSubstituteFor("mysql"))
          .withDatabaseName("POLARIS_SCHEMA");

  private static DatasourceOperations datasourceOperations;

  @BeforeAll
  static void createSchema() throws SQLException {
    MysqlDataSource dataSource = new MysqlDataSource();
    dataSource.setUrl(MYSQL.getJdbcUrl());
    dataSource.setUser(MYSQL.getUsername());
    dataSource.setPassword(MYSQL.getPassword());

    datasourceOperations =
        new DatasourceOperations(
            dataSource,
            new TestConfiguration(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(DatabaseType.MYSQL.getDisplayName())));
    datasourceOperations.executeScript(
        DatabaseType.MYSQL.openInitScriptResource(DatabaseType.MYSQL.getLatestSchemaVersion()));
  }

  @Test
  void deleteKeyedOnTheWholeRowMatchesTheStoredJson() throws SQLException {
    ModelPolicyMappingRecord record =
        ModelPolicyMappingRecord.builder()
            .targetCatalogId(1L)
            .targetId(2L)
            .policyTypeCode(3)
            .policyCatalogId(4L)
            .policyId(5L)
            .parameters("{\"retention\":\"30days\",\"scope\":\"ns\"}")
            .build();

    // `values()` order follows ALL_COLUMNS, which is what generateInsertQuery expects.
    Map<String, Object> row = record.toMap(DatabaseType.MYSQL);
    datasourceOperations.executeUpdate(
        QueryGenerator.generateInsertQuery(
            ModelPolicyMappingRecord.ALL_COLUMNS,
            ModelPolicyMappingRecord.TABLE_NAME,
            List.copyOf(row.values()),
            REALM_ID));

    // The whole row as the WHERE clause, as QueryGeneratorTest#testGenerateDeleteQuery_byObject
    // pins it and JdbcBasePersistenceImpl#deleteFromGrantRecords still uses it.
    row.put("realm_id", REALM_ID);
    int deleted =
        datasourceOperations.executeUpdate(
            new QueryGenerator(DatabaseType.MYSQL)
                .generateDeleteQuery(
                    ModelPolicyMappingRecord.ALL_COLUMNS,
                    ModelPolicyMappingRecord.TABLE_NAME,
                    row));

    assertEquals(1, deleted, "JSON column in the WHERE clause must match the stored row");
  }
}

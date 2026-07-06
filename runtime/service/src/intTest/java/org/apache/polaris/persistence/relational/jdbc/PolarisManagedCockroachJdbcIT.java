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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.test.commons.CockroachRelationalJdbcLifeCycleManagement;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(PolarisManagedCockroachJdbcIT.Profile.class)
class PolarisManagedCockroachJdbcIT {

  @Inject DatasourceOperations datasourceOperations;
  @Inject MetaStoreManagerFactory metaStoreManagerFactory;

  @Test
  void managedCockroachDatasourceBootstrapsSchema() throws Exception {
    assertThat(metaStoreManagerFactory).isInstanceOf(JdbcMetaStoreManagerFactory.class);
    assertThat(datasourceOperations.ownsDataSource()).isTrue();
    assertThat(datasourceOperations.getDatabaseType()).isEqualTo(DatabaseType.COCKROACHDB);

    AtomicInteger schemaVersion = new AtomicInteger();
    AtomicInteger bootstrappedPrincipals = new AtomicInteger();

    datasourceOperations.runWithinTransaction(
        connection -> {
          try (PreparedStatement statement =
                  connection.prepareStatement(
                      "SELECT version_value FROM version WHERE version_key = 'version'");
              ResultSet resultSet = statement.executeQuery()) {
            assertThat(resultSet.next()).isTrue();
            schemaVersion.set(resultSet.getInt(1));
          }

          try (PreparedStatement statement =
                  connection.prepareStatement(
                      "SELECT COUNT(*) FROM principal_authentication_data");
              ResultSet resultSet = statement.executeQuery()) {
            assertThat(resultSet.next()).isTrue();
            bootstrappedPrincipals.set(resultSet.getInt(1));
          }

          return true;
        });

    assertThat(schemaVersion.get()).isEqualTo(4);
    assertThat(bootstrappedPrincipals.get()).isPositive();
  }

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("polaris.persistence.auto-bootstrap-types", "relational-jdbc");
      config.put("polaris.persistence.relational.jdbc.database-type", "cockroachdb");
      return Map.copyOf(config);
    }

    @Override
    public List<QuarkusTestProfile.TestResourceEntry> testResources() {
      return List.of(
          new QuarkusTestProfile.TestResourceEntry(
              CockroachRelationalJdbcLifeCycleManagement.class,
              Map.of(
                  CockroachRelationalJdbcLifeCycleManagement.POLARIS_MANAGED_DATASOURCE, "true")));
    }
  }
}

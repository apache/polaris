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

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.Profiles;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(PolarisManagedJdbcIntegrationTest.Profile.class)
class PolarisManagedJdbcIntegrationTest {

  @Inject DatasourceOperations datasourceOperations;
  @Inject MetaStoreManagerFactory metaStoreManagerFactory;

  @Test
  void managedJdbcDatasourceBootstrapsSchema() throws Exception {
    assertThat(metaStoreManagerFactory).isInstanceOf(JdbcMetaStoreManagerFactory.class);
    assertThat(datasourceOperations.ownsDataSource()).isTrue();
    assertThat(datasourceOperations.getDatabaseType()).isEqualTo(DatabaseType.H2);

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

  public static class Profile extends Profiles.DefaultProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("quarkus.datasource.active", "false")
          .put("polaris.persistence.type", "relational-jdbc")
          .put("polaris.persistence.auto-bootstrap-types", "relational-jdbc")
          .put(
              "polaris.persistence.relational.jdbc.jdbc-url",
              "jdbc:h2:mem:polaris_managed_jdbc_it;DB_CLOSE_DELAY=-1;"
                  + "MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE")
          .put("polaris.persistence.relational.jdbc.driver", "org.h2.Driver")
          .put("polaris.persistence.relational.jdbc.username", "sa")
          .put("polaris.persistence.relational.jdbc.maximum-pool-size", "2")
          .put("polaris.persistence.relational.jdbc.minimum-idle", "1")
          .put("polaris.persistence.relational.jdbc.connection-timeout-in-ms", "1000")
          .build();
    }
  }
}

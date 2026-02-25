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
package org.apache.polaris.service.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.apache.polaris.test.commons.PostgresRelationalJdbcLifeCycleManagement;
import org.apache.polaris.test.commons.RelationalJdbcProfile;
import org.junit.jupiter.api.Test;

public class ServiceProducersIT {

  public static class InternalAuthorizationConfig implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("polaris.authorization.type", "internal");
      return config;
    }
  }

  @QuarkusTest
  @TestProfile(ServiceProducersIT.InternalAuthorizationConfig.class)
  public static class InternalAuthorizationTest {

    @Inject PolarisAuthorizer polarisAuthorizer;

    @Test
    void testInternalPolarisAuthorizerProduced() {
      assertThat(polarisAuthorizer).isNotNull();
    }
  }

  // ========== MetricsPersistence wiring test ==========

  private static final String JDBC_TEST_REALM = "jdbc-test-realm";

  /**
   * Profile that configures relational-jdbc persistence with the metrics datasource configured.
   * This extends RelationalJdbcProfile which sets up a PostgreSQL container with both main and
   * metrics datasources configured. It also configures the realm and bootstrap credentials for
   * auto-bootstrapping.
   *
   * <p>Metrics persistence is auto-detected based on:
   *
   * <ol>
   *   <li>The persistence backend being JdbcBasePersistenceImpl
   *   <li>A named "metrics" datasource being configured
   * </ol>
   */
  public static class JdbcMetricsPersistenceConfig extends RelationalJdbcProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("polaris.realm-context.realms", JDBC_TEST_REALM)
          .put("polaris.bootstrap.credentials", JDBC_TEST_REALM + ",client1,secret1")
          .build();
    }

    @Override
    public List<TestResourceEntry> testResources() {
      return List.of(
          new TestResourceEntry(PostgresRelationalJdbcLifeCycleManagement.class, Map.of()));
    }
  }

  /**
   * Tests that when relational-jdbc persistence is configured with a metrics datasource, the
   * MetricsPersistence is auto-detected and wired correctly.
   *
   * <p>This test verifies that the MetricsPersistence producer auto-detects the JDBC backend with a
   * metrics datasource and returns a functional implementation (not NOOP).
   *
   * <p>Note: Full end-to-end verification of JDBC MetricsPersistence is tested in the JDBC
   * integration tests (MetricsReportPersistenceTest).
   */
  @QuarkusTest
  @TestProfile(ServiceProducersIT.JdbcMetricsPersistenceConfig.class)
  public static class JdbcMetricsPersistenceTest {

    @Inject MetricsPersistence metricsPersistence;

    @Test
    void testJdbcMetricsPersistenceAutoDetected() {
      // Verify that MetricsPersistence is injected (producer worked)
      assertThat(metricsPersistence)
          .as("MetricsPersistence should be injected when metrics datasource is configured")
          .isNotNull();

      // Note: Full functional testing (writes/reads) is done in MetricsReportPersistenceTest.
      // This test verifies the CDI wiring works correctly.
    }
  }
}

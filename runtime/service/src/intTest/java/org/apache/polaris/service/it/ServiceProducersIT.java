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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.apache.polaris.core.persistence.metrics.MetricsQueryCriteria;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
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

  // ========== MetricsPersistence wiring tests ==========

  /**
   * Profile that explicitly sets the metrics persistence type to "noop". This verifies that the
   * configuration property {@code polaris.persistence.metrics.type} is correctly wired to the
   * ServiceProducers and selects the appropriate implementation.
   */
  public static class NoopMetricsPersistenceConfig implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("polaris.persistence.metrics.type", "noop");
      return config;
    }
  }

  /**
   * Tests that when {@code polaris.persistence.metrics.type=noop}, the injected MetricsPersistence
   * is the NoOpMetricsPersistence implementation.
   */
  @QuarkusTest
  @TestProfile(ServiceProducersIT.NoopMetricsPersistenceConfig.class)
  public static class NoopMetricsPersistenceTest {

    @Inject MetricsPersistence metricsPersistence;

    @Test
    void testNoopMetricsPersistenceProduced() {
      assertThat(metricsPersistence).isNotNull();

      // Verify it's the NOOP implementation by checking behavior:
      // NOOP implementation returns empty pages for queries
      MetricsQueryCriteria criteria =
          MetricsQueryCriteria.builder().catalogId(1L).tableId(1L).build();
      Page<?> scanPage = metricsPersistence.queryScanReports(criteria, PageToken.fromLimit(10));
      Page<?> commitPage = metricsPersistence.queryCommitReports(criteria, PageToken.fromLimit(10));

      assertThat(scanPage.items())
          .as("NOOP implementation should return empty scan reports")
          .isEmpty();
      assertThat(commitPage.items())
          .as("NOOP implementation should return empty commit reports")
          .isEmpty();
    }
  }

  /**
   * Profile that uses default configuration (no explicit metrics persistence type). This verifies
   * that the default value "noop" is correctly applied via {@code @WithDefault("noop")}.
   */
  public static class DefaultMetricsPersistenceConfig implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      // No metrics persistence config - should default to "noop"
      return new HashMap<>();
    }
  }

  /**
   * Tests that when no {@code polaris.persistence.metrics.type} is configured, the default "noop"
   * is used and the NoOpMetricsPersistence is injected.
   */
  @QuarkusTest
  @TestProfile(ServiceProducersIT.DefaultMetricsPersistenceConfig.class)
  public static class DefaultMetricsPersistenceTest {

    @Inject MetricsPersistence metricsPersistence;

    @Test
    void testDefaultMetricsPersistenceIsNoop() {
      assertThat(metricsPersistence).isNotNull();

      // Verify it's the NOOP implementation by checking behavior:
      // NOOP implementation returns empty pages for queries
      MetricsQueryCriteria criteria =
          MetricsQueryCriteria.builder().catalogId(1L).tableId(1L).build();
      Page<?> scanPage = metricsPersistence.queryScanReports(criteria, PageToken.fromLimit(10));

      assertThat(scanPage.items())
          .as("Default (NOOP) implementation should return empty scan reports")
          .isEmpty();
    }
  }
}

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
package org.apache.polaris.service.quarkus;

import static org.apache.polaris.service.context.TestRealmIdResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import org.apache.polaris.service.quarkus.TimedApplicationEventListenerTest.Profile;
import org.apache.polaris.service.quarkus.test.PolarisIntegrationTestFixture;
import org.apache.polaris.service.quarkus.test.PolarisIntegrationTestHelper;
import org.apache.polaris.service.quarkus.test.TestEnvironment;
import org.apache.polaris.service.quarkus.test.TestEnvironmentExtension;
import org.apache.polaris.service.quarkus.test.TestMetricsUtil;
import org.hawkular.agent.prometheus.types.MetricFamily;
import org.hawkular.agent.prometheus.types.Summary;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestEnvironmentExtension.class)
@TestProfile(Profile.class)
public class TimedApplicationEventListenerTest {

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("polaris.metrics.tags.environment", "prod");
    }
  }

  private static final int ERROR_CODE = Response.Status.NOT_FOUND.getStatusCode();
  private static final String ENDPOINT = "api/management/v1/principals";
  private static final String METRIC_NAME = "polaris_principals_getPrincipal_seconds";

  @Inject PolarisIntegrationTestHelper helper;
  @Inject MeterRegistry registry;

  private TestEnvironment testEnv;
  private PolarisIntegrationTestFixture fixture;

  @BeforeAll
  public void createFixture(TestEnvironment testEnv, TestInfo testInfo) {
    this.testEnv = testEnv;
    fixture = helper.createFixture(testEnv, testInfo);
  }

  @BeforeEach
  public void clearMetrics() {
    registry.clear();
  }

  @Test
  public void testMetricsEmittedOnSuccessfulRequest() {
    sendSuccessfulRequest();
    Map<String, MetricFamily> allMetrics =
        TestMetricsUtil.fetchMetrics(fixture.client, testEnv.baseManagementUri());
    assertThat(allMetrics).containsKey(METRIC_NAME);
    assertThat(allMetrics.get(METRIC_NAME).getMetrics())
        .satisfiesOnlyOnce(
            metric -> {
              assertThat(metric.getLabels())
                  .contains(
                      Map.entry("application", "Polaris"),
                      Map.entry("environment", "prod"),
                      Map.entry("realm_id", fixture.realm),
                      Map.entry(
                          "class", "org.apache.polaris.service.admin.api.PolarisPrincipalsApi"),
                      Map.entry("exception", "none"),
                      Map.entry("method", "getPrincipal"));
              assertThat(metric)
                  .asInstanceOf(type(Summary.class))
                  .extracting(Summary::getSampleCount)
                  .isEqualTo(1L);
            });
  }

  @Test
  public void testMetricsEmittedOnFailedRequest() {
    sendFailingRequest();
    Map<String, MetricFamily> allMetrics =
        TestMetricsUtil.fetchMetrics(fixture.client, testEnv.baseManagementUri());
    assertThat(allMetrics).containsKey(METRIC_NAME);
    assertThat(allMetrics.get(METRIC_NAME).getMetrics())
        .satisfiesOnlyOnce(
            metric -> {
              assertThat(metric.getLabels())
                  .contains(
                      Map.entry("application", "Polaris"),
                      Map.entry("environment", "prod"),
                      Map.entry("realm_id", fixture.realm),
                      Map.entry(
                          "class", "org.apache.polaris.service.admin.api.PolarisPrincipalsApi"),
                      Map.entry("exception", "NotFoundException"),
                      Map.entry("method", "getPrincipal"));
              assertThat(metric)
                  .asInstanceOf(type(Summary.class))
                  .extracting(Summary::getSampleCount)
                  .isEqualTo(1L);
            });
  }

  private int sendRequest(String principalName) {
    try (Response response =
        fixture
            .client
            .target(String.format("%s/%s/%s", testEnv.baseUri(), ENDPOINT, principalName))
            .request("application/json")
            .header("Authorization", "Bearer " + fixture.adminToken)
            .header(REALM_PROPERTY_KEY, fixture.realm)
            .get()) {
      return response.getStatus();
    }
  }

  private void sendSuccessfulRequest() {
    Assertions.assertEquals(
        Response.Status.OK.getStatusCode(),
        sendRequest(fixture.snowmanCredentials.identifier().principalName()));
  }

  private void sendFailingRequest() {
    Assertions.assertEquals(ERROR_CODE, sendRequest("notarealprincipal"));
  }
}

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
package org.apache.polaris.service.quarkus.metrics;

import static org.apache.polaris.service.context.TestRealmContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.service.quarkus.test.PolarisIntegrationTestFixture;
import org.apache.polaris.service.quarkus.test.PolarisIntegrationTestHelper;
import org.apache.polaris.service.quarkus.test.TestEnvironment;
import org.apache.polaris.service.quarkus.test.TestEnvironmentExtension;
import org.apache.polaris.service.quarkus.test.TestMetricsUtil;
import org.awaitility.Awaitility;
import org.hawkular.agent.prometheus.types.MetricFamily;
import org.hawkular.agent.prometheus.types.Summary;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestEnvironmentExtension.class)
public abstract class MetricsTestBase {

  private static final int ERROR_CODE = Response.Status.NOT_FOUND.getStatusCode();
  private static final String ENDPOINT = "api/management/v1/principals";
  private static final String API_METRIC_NAME = "polaris_principals_getPrincipal_seconds";
  private static final String HTTP_METRIC_NAME = "http_server_requests_seconds";

  @Inject PolarisIntegrationTestHelper helper;
  @Inject MeterRegistry registry;
  @Inject QuarkusMetricsConfiguration metricsConfiguration;

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

  private Map<String, MetricFamily> fetchMetrics(String endpoint) {
    AtomicReference<Map<String, MetricFamily>> value = new AtomicReference<>();
    Awaitility.await()
        .atMost(Duration.ofMinutes(2))
        .untilAsserted(
            () -> {
              value.set(
                  TestMetricsUtil.fetchMetrics(
                      fixture.client, testEnv.baseManagementUri(), endpoint));
              assertThat(value.get()).containsKey(API_METRIC_NAME);
              assertThat(value.get()).containsKey(HTTP_METRIC_NAME);
            });
    return value.get();
  }

  @ParameterizedTest
  @ValueSource(strings = {"%s/metrics", "%s/q/metrics"})
  public void testMetricsEmittedOnSuccessfulRequest(String endpoint) {
    sendSuccessfulRequest();
    Map<String, MetricFamily> allMetrics = fetchMetrics(endpoint);
    assertThat(allMetrics).containsKey(API_METRIC_NAME);
    assertThat(allMetrics.get(API_METRIC_NAME).getMetrics())
        .satisfiesOnlyOnce(
            metric -> {
              assertThat(metric.getLabels())
                  .contains(
                      Map.entry("application", "Polaris"),
                      Map.entry("environment", "prod"),
                      Map.entry(
                          "realm_id",
                          metricsConfiguration.realmIdTag().enableInApiMetrics()
                              ? fixture.realm
                              : ""),
                      Map.entry(
                          "class", "org.apache.polaris.service.admin.api.PolarisPrincipalsApi"),
                      Map.entry("exception", "none"),
                      Map.entry("method", "getPrincipal"));
              assertThat(metric)
                  .asInstanceOf(type(Summary.class))
                  .extracting(Summary::getSampleCount)
                  .isEqualTo(1L);
            });
    assertThat(allMetrics).containsKey(HTTP_METRIC_NAME);
    assertThat(allMetrics.get(HTTP_METRIC_NAME).getMetrics())
        .satisfiesOnlyOnce(
            metric -> {
              assertThat(metric.getLabels())
                  .contains(
                      Map.entry("application", "Polaris"),
                      Map.entry("environment", "prod"),
                      Map.entry("method", "GET"),
                      Map.entry("outcome", "SUCCESS"),
                      Map.entry("status", "200"),
                      Map.entry("uri", "/api/management/v1/principals/{principalName}"));
              if (metricsConfiguration.realmIdTag().enableInHttpMetrics()) {
                assertThat(metric.getLabels()).containsEntry("realm_id", fixture.realm);
              } else {
                assertThat(metric.getLabels()).doesNotContainKey("realm_id");
              }
              assertThat(metric)
                  .asInstanceOf(type(Summary.class))
                  .extracting(Summary::getSampleCount)
                  .isEqualTo(1L);
            });
  }

  @ParameterizedTest
  @ValueSource(strings = {"%s/metrics", "%s/q/metrics"})
  public void testMetricsEmittedOnFailedRequest(String endpoint) {
    sendFailingRequest();
    Map<String, MetricFamily> allMetrics = fetchMetrics(endpoint);
    assertThat(allMetrics).containsKey(API_METRIC_NAME);
    assertThat(allMetrics.get(API_METRIC_NAME).getMetrics())
        .satisfiesOnlyOnce(
            metric -> {
              assertThat(metric.getLabels())
                  .contains(
                      Map.entry("application", "Polaris"),
                      Map.entry("environment", "prod"),
                      Map.entry(
                          "realm_id",
                          metricsConfiguration.realmIdTag().enableInApiMetrics()
                              ? fixture.realm
                              : ""),
                      Map.entry(
                          "class", "org.apache.polaris.service.admin.api.PolarisPrincipalsApi"),
                      Map.entry("exception", "NotFoundException"),
                      Map.entry("method", "getPrincipal"));
              assertThat(metric)
                  .asInstanceOf(type(Summary.class))
                  .extracting(Summary::getSampleCount)
                  .isEqualTo(1L);
            });
    assertThat(allMetrics.get(HTTP_METRIC_NAME).getMetrics())
        .satisfiesOnlyOnce(
            metric -> {
              assertThat(metric.getLabels())
                  .contains(
                      Map.entry("application", "Polaris"),
                      Map.entry("environment", "prod"),
                      Map.entry("method", "GET"),
                      Map.entry("outcome", "CLIENT_ERROR"),
                      Map.entry("status", "404"),
                      Map.entry("uri", "/api/management/v1/principals/{principalName}"));
              if (metricsConfiguration.realmIdTag().enableInHttpMetrics()) {
                assertThat(metric.getLabels()).containsEntry("realm_id", fixture.realm);
              } else {
                assertThat(metric.getLabels()).doesNotContainKey("realm_id");
              }
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

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
package org.apache.polaris.service.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.service.it.env.ClientPrincipal;
import org.apache.polaris.service.it.env.PlatformApiEndpoints;
import org.apache.polaris.service.it.env.PlatformMetricsApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.ratelimiter.MockRateLimiter;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.awaitility.Awaitility;
import org.hawkular.agent.prometheus.types.Histogram;
import org.hawkular.agent.prometheus.types.Metric;
import org.hawkular.agent.prometheus.types.MetricFamily;
import org.hawkular.agent.prometheus.types.Summary;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(PolarisIntegrationTestExtension.class)
public abstract class MetricsTestBase {

  private static final int ERROR_CODE = Response.Status.NOT_FOUND.getStatusCode();
  private static final String API_METRIC_NAME = "polaris_principals_getPrincipal_seconds";
  private static final String HTTP_METRIC_NAME = "http_server_requests_seconds";

  @Inject MeterRegistry registry;
  @Inject MetricsConfiguration metricsConfiguration;

  private PlatformMetricsApi platformMetricsApi;
  private PolarisApiEndpoints polarisEndpoints;
  private PolarisClient client;
  private ClientPrincipal principal;
  private String adminToken;

  @BeforeAll
  public void setUp(
      PolarisApiEndpoints polarisEndpoints,
      PlatformApiEndpoints quarkusEndpoints,
      ClientPrincipal principal) {
    MockRateLimiter.allowProceed = true;
    this.polarisEndpoints = polarisEndpoints;
    this.principal = principal;
    client = PolarisClient.polarisClient(polarisEndpoints, quarkusEndpoints);
    platformMetricsApi = client.platformMetricsApi();
    adminToken = client.obtainToken(principal.credentials());
  }

  @AfterAll
  public void tearDown() throws Exception {
    if (client != null) {
      client.close();
    }
  }

  @BeforeEach
  public void clearMetrics() {
    registry.clear();
  }

  private Map<String, MetricFamily> fetchMetrics() {
    AtomicReference<Map<String, MetricFamily>> value = new AtomicReference<>();
    Awaitility.await()
        .atMost(Duration.ofMinutes(2))
        .untilAsserted(
            () -> {
              value.set(platformMetricsApi.fetchMetrics());
              assertThat(value.get()).containsKey(API_METRIC_NAME);
              assertThat(value.get()).containsKey(HTTP_METRIC_NAME);
            });
    return value.get();
  }

  @Test
  public void testMetricsEmittedOnSuccessfulRequest() {
    sendSuccessfulRequest();
    Map<String, MetricFamily> allMetrics = fetchMetrics();
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
                              ? polarisEndpoints.realmId()
                              : ""),
                      Map.entry(
                          "principal",
                          metricsConfiguration.userPrincipalTag().enableInApiMetrics()
                              ? "root"
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
                assertThat(metric.getLabels())
                    .containsEntry("realm_id", polarisEndpoints.realmId());
              } else {
                assertThat(metric.getLabels()).doesNotContainKey("realm_id");
              }
              assertThat(httpSampleCount(metric)).isEqualTo(1L);
            });
  }

  @Test
  public void testMetricsEmittedOnFailedRequest() {
    sendFailingRequest();
    Map<String, MetricFamily> allMetrics = fetchMetrics();
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
                              ? polarisEndpoints.realmId()
                              : ""),
                      Map.entry(
                          "principal",
                          metricsConfiguration.userPrincipalTag().enableInApiMetrics()
                              ? "root"
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
                assertThat(metric.getLabels())
                    .containsEntry("realm_id", polarisEndpoints.realmId());
              } else {
                assertThat(metric.getLabels()).doesNotContainKey("realm_id");
              }
              assertThat(httpSampleCount(metric)).isEqualTo(1L);
            });
  }

  @Test
  public void testHttpHistogramBucketsConfig() {
    sendSuccessfulRequest();
    Map<String, MetricFamily> allMetrics = fetchMetrics();
    boolean histogramEnabled = metricsConfiguration.httpServerRequests().publishHistogram();
    assertThat(allMetrics.get(HTTP_METRIC_NAME).getMetrics())
        .allSatisfy(
            metrics -> {
              if (histogramEnabled) {
                assertThat(metrics)
                    .asInstanceOf(type(Histogram.class))
                    .extracting(Histogram::getBuckets)
                    .asInstanceOf(InstanceOfAssertFactories.LIST)
                    .isNotEmpty();
              } else {
                assertThat(metrics).isInstanceOf(Summary.class);
              }
            });
  }

  public static long httpSampleCount(Metric metrics) {
    if (metrics instanceof Histogram histogram) {
      return histogram.getSampleCount();
    }
    return ((Summary) metrics).getSampleCount();
  }

  private int sendRequest(String principalName) {
    try (Response response =
        client
            .managementApi(adminToken)
            .request("v1/principals/{name}", Map.of("name", principalName))
            .get()) {
      return response.getStatus();
    }
  }

  private void sendSuccessfulRequest() {
    Assertions.assertEquals(
        Response.Status.OK.getStatusCode(), sendRequest(principal.principalName()));
  }

  private void sendFailingRequest() {
    Assertions.assertEquals(ERROR_CODE, sendRequest("notarealprincipal"));
  }
}

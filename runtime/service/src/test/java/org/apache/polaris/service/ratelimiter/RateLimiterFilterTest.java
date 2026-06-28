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
package org.apache.polaris.service.ratelimiter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ErrorResponseParser;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.TestPolarisEventListener;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.PlatformApiEndpoints;
import org.apache.polaris.service.it.env.PlatformMetricsApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.ratelimiter.RateLimiterFilterTest.Profile;
import org.hawkular.agent.prometheus.types.Histogram;
import org.hawkular.agent.prometheus.types.Metric;
import org.hawkular.agent.prometheus.types.MetricFamily;
import org.hawkular.agent.prometheus.types.Summary;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;

/** Main integration tests for rate limiting */
@QuarkusTest
@TestInstance(Lifecycle.PER_CLASS)
@TestProfile(Profile.class)
@ExtendWith(PolarisIntegrationTestExtension.class)
public class RateLimiterFilterTest {

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Set<Class<?>> getEnabledAlternatives() {
      return Set.of(MockTokenBucketFactory.class);
    }

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("polaris.rate-limiter.filter.type", "mock")
          .put("polaris.rate-limiter.token-bucket.type", "default")
          .put("polaris.metrics.tags.environment", "prod")
          .put("polaris.realm-context.realms", "POLARIS,POLARIS2")
          .put("polaris.metrics.realm-id-tag.enable-in-api-metrics", "true")
          .put("polaris.metrics.realm-id-tag.enable-in-http-metrics", "true")
          .put("polaris.authentication.token-broker.type", "symmetric-key")
          .put("polaris.authentication.token-broker.symmetric-key.secret", "secret")
          .put("polaris.event-listener.types", "test")
          .build();
    }
  }

  @Inject MeterRegistry meterRegistry;

  @Inject
  @Identifier("test")
  TestPolarisEventListener polarisEventListener;

  private PlatformMetricsApi platformMetricsApi;
  private PolarisApiEndpoints polarisEndpoints;
  private PolarisClient client;
  private String adminToken;

  @BeforeAll
  public void setUp(
      PolarisApiEndpoints polarisEndpoints,
      PlatformApiEndpoints quarkusEndpoints,
      ClientCredentials credentials) {
    MockRateLimiter.allowProceed = true;
    this.polarisEndpoints = polarisEndpoints;
    client = PolarisClient.polarisClient(polarisEndpoints, quarkusEndpoints);
    platformMetricsApi = client.platformMetricsApi();
    adminToken = client.obtainToken(credentials);
  }

  @AfterAll
  public void tearDown() throws Exception {
    if (client != null) {
      client.close();
    }
  }

  @BeforeEach
  public void resetMeterRegistry() {
    MockRateLimiter.allowProceed = true;
    meterRegistry.clear();
    polarisEventListener.clear();
  }

  @Test
  public void testRateLimiter() {
    Consumer<Status> requestAsserter =
        constructRequestAsserter(polarisEndpoints, adminToken, "POLARIS");

    for (int i = 0; i < 3; i++) {
      MockRateLimiter.allowProceed = true;
      requestAsserter.accept(Status.OK);
      MockRateLimiter.allowProceed = false;
      requestAsserter.accept(Status.TOO_MANY_REQUESTS);
    }

    // Ensure that a different realm identifier gets a separate limit
    MockRateLimiter.allowProceed = true;
    Consumer<Status> requestAsserter2 =
        constructRequestAsserter(polarisEndpoints, adminToken, "POLARIS2");
    requestAsserter2.accept(Status.OK);
  }

  @Test
  public void testRateLimitedResponseHasIcebergErrorBody() {
    MockRateLimiter.allowProceed = false;
    try (Response response = client.managementApi(adminToken).request("v1/principal-roles").get()) {
      assertThat(response.getStatus()).isEqualTo(Status.TOO_MANY_REQUESTS.getStatusCode());
      assertThat(response.getMediaType()).isEqualTo(MediaType.APPLICATION_JSON_TYPE);

      String body = response.readEntity(String.class);
      ErrorResponse parsed = ErrorResponseParser.fromJson(body);
      assertThat(parsed.code()).isEqualTo(Status.TOO_MANY_REQUESTS.getStatusCode());
      assertThat(parsed.type()).isEqualTo("TooManyRequestsException");
      assertThat(parsed.message()).contains("Rate exceeded");
    }
  }

  @Test
  public void testMetricsAreEmittedWhenRateLimiting() {
    Consumer<Status> requestAsserter =
        constructRequestAsserter(polarisEndpoints, adminToken, polarisEndpoints.realmId());

    for (int i = 0; i < 3; i++) {
      MockRateLimiter.allowProceed = true;
      requestAsserter.accept(Status.OK);
      MockRateLimiter.allowProceed = false;
      requestAsserter.accept(Status.TOO_MANY_REQUESTS);
    }

    PolarisEvent event = polarisEventListener.getLatest(PolarisEventType.BEFORE_LIMIT_REQUEST_RATE);
    assertThat(event.attributes().getRequired(EventAttributes.HTTP_METHOD)).isEqualTo("GET");

    // Examples of expected metrics:
    // http_server_requests_seconds_count{application="Polaris",environment="prod",method="GET",outcome="CLIENT_ERROR",realm_id="org_apache_polaris_service_ratelimiter_RateLimiterFilterTest",status="429",uri="/api/management/v1/principal-roles"} 1.0
    // polaris_principal_roles_listPrincipalRoles_seconds_count{application="Polaris",class="org.apache.polaris.service.admin.api.PolarisPrincipalRolesApi",environment="prod",exception="none",method="listPrincipalRoles"} 50.0

    Map<String, MetricFamily> metrics = platformMetricsApi.fetchMetrics();

    assertThat(metrics)
        .isNotEmpty()
        .containsKey("http_server_requests_seconds")
        .containsKey("polaris_principal_roles_listPrincipalRoles_seconds");

    assertThat(metrics.get("http_server_requests_seconds").getMetrics())
        .satisfiesOnlyOnce(
            metric -> {
              assertThat(metric.getLabels())
                  .contains(
                      Map.entry("application", "Polaris"),
                      Map.entry("environment", "prod"),
                      Map.entry("realm_id", polarisEndpoints.realmId()),
                      Map.entry("method", "GET"),
                      Map.entry("outcome", "CLIENT_ERROR"),
                      Map.entry("status", String.valueOf(Status.TOO_MANY_REQUESTS.getStatusCode())),
                      Map.entry("uri", "/api/management/v1/principal-roles"));
              assertThat(httpSampleCount(metric)).isEqualTo(3L);
            });

    assertThat(metrics.get("polaris_principal_roles_listPrincipalRoles_seconds").getMetrics())
        .satisfiesOnlyOnce(
            metric -> {
              assertThat(metric.getLabels())
                  .contains(
                      Map.entry("application", "Polaris"),
                      Map.entry("environment", "prod"),
                      Map.entry("realm_id", polarisEndpoints.realmId()),
                      Map.entry(
                          "class", "org.apache.polaris.service.admin.api.PolarisPrincipalRolesApi"),
                      Map.entry("exception", "none"),
                      Map.entry("method", "listPrincipalRoles"));
              assertThat(metric)
                  .asInstanceOf(type(Summary.class))
                  .extracting(Summary::getSampleCount)
                  .isEqualTo(3L);
            });
  }

  private static long httpSampleCount(Metric metric) {
    if (metric instanceof Histogram histogram) {
      return histogram.getSampleCount();
    }
    return ((Summary) metric).getSampleCount();
  }

  /**
   * Constructs a function that makes a request to list all principal roles and asserts the status
   * of the response. This is a relatively simple type of request that can be used for validating
   * whether the rate limiter intervenes.
   */
  private static Consumer<Status> constructRequestAsserter(
      PolarisApiEndpoints endpoints, String adminToken, String realm) {
    return (Status status) -> {
      try (Client client = ClientBuilder.newBuilder().build();
          Response response =
              client
                  .target(String.format("%s/v1/principal-roles", endpoints.managementApiEndpoint()))
                  .request("application/json")
                  .header("Authorization", "Bearer " + adminToken)
                  .header("Polaris-Realm", realm)
                  .get()) {
        assertThat(response).returns(status.getStatusCode(), Response::getStatus);
      }
    };
  }
}

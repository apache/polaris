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
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response.Status;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.events.listeners.TestPolarisEventListener;
import org.apache.polaris.service.ratelimiter.RateLimiterFilterTest.Profile;
import org.apache.polaris.service.test.PolarisIntegrationTestFixture;
import org.apache.polaris.service.test.PolarisIntegrationTestHelper;
import org.apache.polaris.service.test.TestEnvironment;
import org.apache.polaris.service.test.TestEnvironmentExtension;
import org.apache.polaris.service.test.TestMetricsUtil;
import org.hawkular.agent.prometheus.types.MetricFamily;
import org.hawkular.agent.prometheus.types.Summary;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;

/** Main integration tests for rate limiting */
@QuarkusTest
@TestInstance(Lifecycle.PER_CLASS)
@TestProfile(Profile.class)
@ExtendWith(TestEnvironmentExtension.class)
public class RateLimiterFilterTest {

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Set<Class<?>> getEnabledAlternatives() {
      return Set.of(MockTokenBucketFactory.class);
    }

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("polaris.rate-limiter.filter.type", "default")
          .put("polaris.rate-limiter.token-bucket.type", "default")
          .put(
              "polaris.rate-limiter.token-bucket.requests-per-second",
              String.valueOf(REQUESTS_PER_SECOND))
          .put("polaris.rate-limiter.token-bucket.window", WINDOW.toString())
          .put("polaris.metrics.tags.environment", "prod")
          .put("polaris.metrics.realm-id-tag.enable-in-api-metrics", "true")
          .put("polaris.metrics.realm-id-tag.enable-in-http-metrics", "true")
          .put("polaris.realm-context.type", "test")
          .put("polaris.authentication.token-broker.type", "symmetric-key")
          .put("polaris.authentication.token-broker.symmetric-key.secret", "secret")
          .put("polaris.event-listener.type", "test")
          .build();
    }
  }

  private static final long REQUESTS_PER_SECOND = 5;
  private static final Duration WINDOW = Duration.ofSeconds(10);

  @Inject PolarisIntegrationTestHelper helper;
  @Inject MeterRegistry meterRegistry;
  @Inject PolarisEventListener polarisEventListener;

  private TestEnvironment testEnv;
  private PolarisIntegrationTestFixture fixture;

  @BeforeAll
  public void createFixture(TestEnvironment testEnv, TestInfo testInfo) {
    this.testEnv = testEnv;
    fixture = helper.createFixture(testEnv, testInfo);
  }

  @AfterAll
  public void destroyFixture() {
    if (fixture != null) {
      fixture.destroy();
    }
  }

  @BeforeEach
  @AfterEach
  public void resetRateLimiter() {
    MockTokenBucketFactory.CLOCK.add(
        WINDOW.multipliedBy(2)); // Clear any counters from before/after this test
  }

  @BeforeEach
  public void resetMeterRegistry() {
    meterRegistry.clear();
  }

  @Test
  public void testRateLimiter() {
    Consumer<Status> requestAsserter =
        TestUtil.constructRequestAsserter(testEnv, fixture, fixture.realm);

    for (int i = 0; i < REQUESTS_PER_SECOND * WINDOW.toSeconds(); i++) {
      requestAsserter.accept(Status.OK);
    }
    requestAsserter.accept(Status.TOO_MANY_REQUESTS);

    // Ensure that a different realm identifier gets a separate limit
    Consumer<Status> requestAsserter2 =
        TestUtil.constructRequestAsserter(testEnv, fixture, fixture.realm + "2");
    requestAsserter2.accept(Status.OK);
  }

  @Test
  public void testMetricsAreEmittedWhenRateLimiting() {
    Consumer<Status> requestAsserter =
        TestUtil.constructRequestAsserter(testEnv, fixture, fixture.realm);

    for (int i = 0; i < REQUESTS_PER_SECOND * WINDOW.toSeconds(); i++) {
      requestAsserter.accept(Status.OK);
    }
    requestAsserter.accept(Status.TOO_MANY_REQUESTS);

    PolarisEvent event =
        ((TestPolarisEventListener) polarisEventListener)
            .getLatest(PolarisEventType.BEFORE_LIMIT_REQUEST_RATE);
    assertThat(event.attributes().get(EventAttributes.HTTP_METHOD)).hasValue("GET");

    // Examples of expected metrics:
    // http_server_requests_seconds_count{application="Polaris",environment="prod",method="GET",outcome="CLIENT_ERROR",realm_id="org_apache_polaris_service_ratelimiter_RateLimiterFilterTest",status="429",uri="/api/management/v1/principal-roles"} 1.0
    // polaris_principal_roles_listPrincipalRoles_seconds_count{application="Polaris",class="org.apache.polaris.service.admin.api.PolarisPrincipalRolesApi",environment="prod",exception="none",method="listPrincipalRoles"} 50.0

    Map<String, MetricFamily> metrics =
        TestMetricsUtil.fetchMetrics(fixture.client, testEnv.baseManagementUri());

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
                      Map.entry("realm_id", fixture.realm),
                      Map.entry("method", "GET"),
                      Map.entry("outcome", "CLIENT_ERROR"),
                      Map.entry("status", String.valueOf(Status.TOO_MANY_REQUESTS.getStatusCode())),
                      Map.entry("uri", "/api/management/v1/principal-roles"));
              assertThat(metric)
                  .asInstanceOf(type(Summary.class))
                  .extracting(Summary::getSampleCount)
                  .isEqualTo(1L);
            });

    assertThat(metrics.get("polaris_principal_roles_listPrincipalRoles_seconds").getMetrics())
        .satisfiesOnlyOnce(
            metric -> {
              assertThat(metric.getLabels())
                  .contains(
                      Map.entry("application", "Polaris"),
                      Map.entry("environment", "prod"),
                      Map.entry("realm_id", fixture.realm),
                      Map.entry(
                          "class", "org.apache.polaris.service.admin.api.PolarisPrincipalRolesApi"),
                      Map.entry("exception", "none"),
                      Map.entry("method", "listPrincipalRoles"));
              assertThat(metric)
                  .asInstanceOf(type(Summary.class))
                  .extracting(Summary::getSampleCount)
                  .isEqualTo(REQUESTS_PER_SECOND * WINDOW.toSeconds());
            });
  }
}

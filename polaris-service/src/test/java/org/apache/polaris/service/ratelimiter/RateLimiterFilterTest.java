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

import static org.apache.polaris.core.monitor.PolarisMetricRegistry.*;
import static org.apache.polaris.service.TimedApplicationEventListener.SINGLETON_METRIC_NAME;
import static org.apache.polaris.service.TimedApplicationEventListener.TAG_API_NAME;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.micrometer.core.instrument.Tag;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import org.apache.polaris.service.PolarisApplication;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.test.PolarisConnectionExtension;
import org.apache.polaris.service.test.PolarisRealm;
import org.apache.polaris.service.test.SnowmanCredentialsExtension;
import org.apache.polaris.service.test.TestEnvironmentExtension;
import org.apache.polaris.service.test.TestMetricsUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.threeten.extra.MutableClock;

/** Main integration tests for rate limiting */
@ExtendWith({
  DropwizardExtensionsSupport.class,
  TestEnvironmentExtension.class,
  PolarisConnectionExtension.class,
  SnowmanCredentialsExtension.class
})
public class RateLimiterFilterTest {
  private static final long REQUESTS_PER_SECOND = 5;
  private static final long WINDOW_SECONDS = 10;
  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
          ConfigOverride.config(
              "server.applicationConnectors[0].port",
              "0"), // Bind to random port to support parallelism
          ConfigOverride.config("server.adminConnectors[0].port", "0"),
          ConfigOverride.config("rateLimiter.type", "mock-realm-token-bucket"),
          ConfigOverride.config(
              "rateLimiter.requestsPerSecond", String.valueOf(REQUESTS_PER_SECOND)),
          ConfigOverride.config("rateLimiter.windowSeconds", String.valueOf(WINDOW_SECONDS)));

  private static String userToken;
  private static String realm;
  private static MutableClock clock = MockRealmTokenBucketRateLimiter.CLOCK;

  @BeforeAll
  public static void setup(
      PolarisConnectionExtension.PolarisToken userToken, @PolarisRealm String polarisRealm) {
    realm = polarisRealm;
    RateLimiterFilterTest.userToken = userToken.token();
  }

  @BeforeEach
  @AfterEach
  public void resetRateLimiter() {
    clock.add(
        Duration.ofSeconds(2 * WINDOW_SECONDS)); // Clear any counters from before/after this test
  }

  @Test
  public void testRateLimiter() {
    Consumer<Response.Status> requestAsserter =
        TestUtil.constructRequestAsserter(EXT, userToken, realm);

    for (int i = 0; i < REQUESTS_PER_SECOND * WINDOW_SECONDS; i++) {
      requestAsserter.accept(Response.Status.OK);
    }
    requestAsserter.accept(Response.Status.TOO_MANY_REQUESTS);

    // Ensure that a different realm identifier gets a separate limit
    Consumer<Response.Status> requestAsserter2 =
        TestUtil.constructRequestAsserter(EXT, userToken, realm + "2");
    requestAsserter2.accept(Response.Status.OK);
  }

  @Test
  public void testMetricsAreEmittedWhenRateLimiting() {
    Consumer<Response.Status> requestAsserter =
        TestUtil.constructRequestAsserter(EXT, userToken, realm);

    for (int i = 0; i < REQUESTS_PER_SECOND * WINDOW_SECONDS; i++) {
      requestAsserter.accept(Response.Status.OK);
    }
    requestAsserter.accept(Response.Status.TOO_MANY_REQUESTS);

    assertTrue(
        TestMetricsUtil.getTotalCounter(
                EXT,
                SINGLETON_METRIC_NAME + SUFFIX_ERROR,
                List.of(
                    Tag.of(TAG_API_NAME, "polaris.principal-roles.listPrincipalRoles"),
                    Tag.of(
                        TAG_RESP_CODE,
                        String.valueOf(Response.Status.TOO_MANY_REQUESTS.getStatusCode()))))
            > 0);
  }
}

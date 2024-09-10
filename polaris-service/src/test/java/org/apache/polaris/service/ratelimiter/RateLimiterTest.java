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

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.core.Response;
import java.util.function.Consumer;
import org.apache.polaris.service.PolarisApplication;
import org.apache.polaris.service.PolarisApplicationIntegrationTest;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.test.PolarisConnectionExtension;
import org.apache.polaris.service.test.SnowmanCredentialsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Main integration tests for rate limiting */
@ExtendWith({
  DropwizardExtensionsSupport.class,
  PolarisConnectionExtension.class,
  SnowmanCredentialsExtension.class
})
public class RateLimiterTest {
  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
          ConfigOverride.config(
              "server.applicationConnectors[0].port",
              "0"), // Bind to random port to support parallelism
          ConfigOverride.config("server.adminConnectors[0].port", "0"),
          ConfigOverride.config("rateLimiter.factory.type", "mock"));

  private static String userToken;
  private static String realm;

  @BeforeAll
  public static void setup(PolarisConnectionExtension.PolarisToken userToken) {
    realm = PolarisConnectionExtension.getTestRealm(PolarisApplicationIntegrationTest.class);
    RateLimiterTest.userToken = userToken.token();
  }

  @Test
  public void testRateLimiter() {
    Consumer<Response.Status> requestAsserter =
        TestUtil.constructRequestAsserter(EXT, userToken, realm);
    MockRateLimiterFactory factory =
        (MockRateLimiterFactory)
            (EXT.getConfiguration().getRateLimiterConfig().getRateLimiterFactory());
    long windowMillis = (long) (factory.windowSeconds * 1000);

    MockClock clock = MockRateLimiterFactory.clock;
    clock.setMillis(2 * windowMillis); // Clear any counters from before this test
    for (int i = 0; i < factory.requestsPerSecond * factory.windowSeconds; i++) {
      requestAsserter.accept(Response.Status.OK);
    }
    requestAsserter.accept(Response.Status.TOO_MANY_REQUESTS);

    clock.setMillis(4 * windowMillis); // Clear any counters from this test
  }
}

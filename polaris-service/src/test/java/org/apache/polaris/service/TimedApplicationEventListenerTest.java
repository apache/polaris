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
package org.apache.polaris.service;

import static org.apache.polaris.core.monitor.PolarisMetricRegistry.SUFFIX_COUNTER;
import static org.apache.polaris.core.monitor.PolarisMetricRegistry.SUFFIX_ERROR;
import static org.apache.polaris.service.TimedApplicationEventListener.METRIC_NAME;
import static org.apache.polaris.service.TimedApplicationEventListener.TAG_API_NAME;
import static org.apache.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.micrometer.core.instrument.Tag;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.polaris.core.monitor.PolarisMetricRegistry;
import org.apache.polaris.core.resource.TimedApi;
import org.apache.polaris.service.admin.api.PolarisPrincipalsApi;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.test.PolarisConnectionExtension;
import org.apache.polaris.service.test.PolarisRealm;
import org.apache.polaris.service.test.SnowmanCredentialsExtension;
import org.apache.polaris.service.test.TestEnvironmentExtension;
import org.apache.polaris.service.test.TestMetricsUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({
  DropwizardExtensionsSupport.class,
  TestEnvironmentExtension.class,
  PolarisConnectionExtension.class,
  SnowmanCredentialsExtension.class
})
public class TimedApplicationEventListenerTest {
  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
          ConfigOverride.config(
              "server.applicationConnectors[0].port",
              "0"), // Bind to random port to support parallelism
          ConfigOverride.config(
              "server.adminConnectors[0].port", "0")); // Bind to random port to support parallelism

  private static final String ENDPOINT = "api/management/v1/principals";
  private static final String API_ANNOTATION =
      Arrays.stream(PolarisPrincipalsApi.class.getMethods())
          .filter(m -> m.getName().contains("getPrincipal"))
          .findFirst()
          .orElseThrow()
          .getAnnotation(TimedApi.class)
          .value();

  private static PolarisConnectionExtension.PolarisToken userToken;
  private static String realm;

  @BeforeAll
  public static void setup(
      PolarisConnectionExtension.PolarisToken userToken, @PolarisRealm String realm)
      throws IOException {
    TimedApplicationEventListenerTest.userToken = userToken;
    TimedApplicationEventListenerTest.realm = realm;
  }

  @BeforeEach
  public void clearMetrics() {
    getPolarisMetricRegistry().clear();
  }

  @Test
  public void testMetricsEmittedOnSuccessfulRequest() {
    sendSuccessfulRequest();
    Assertions.assertTrue(getPerApiMetricCount() > 0);
    Assertions.assertTrue(getCommonMetricCount() > 0);
    Assertions.assertEquals(0, getPerApiMetricErrorCount());
    Assertions.assertEquals(0, getCommonMetricErrorCount());
  }

  @Test
  public void testMetricsEmittedOnFailedRequest() {
    sendFailingRequest();
    Assertions.assertTrue(getPerApiMetricCount() > 0);
    Assertions.assertTrue(getCommonMetricCount() > 0);
    Assertions.assertTrue(getPerApiMetricErrorCount() > 0);
    Assertions.assertTrue(getCommonMetricErrorCount() > 0);
  }

  private PolarisMetricRegistry getPolarisMetricRegistry() {
    TimedApplicationEventListener listener =
        (TimedApplicationEventListener)
            EXT.getEnvironment().jersey().getResourceConfig().getSingletons().stream()
                .filter(
                    s ->
                        TimedApplicationEventListener.class
                            .getName()
                            .equals(s.getClass().getName()))
                .findAny()
                .orElseThrow();
    return listener.getMetricRegistry();
  }

  private double getPerApiMetricCount() {
    return TestMetricsUtil.getTotalCounter(
        EXT, API_ANNOTATION + SUFFIX_COUNTER, Collections.emptyList());
  }

  private double getPerApiMetricErrorCount() {
    return TestMetricsUtil.getTotalCounter(
        EXT, API_ANNOTATION + SUFFIX_ERROR, Collections.emptyList());
  }

  private double getCommonMetricCount() {
    return TestMetricsUtil.getTotalCounter(
        EXT,
        METRIC_NAME + SUFFIX_COUNTER,
        Collections.singleton(Tag.of(TAG_API_NAME, API_ANNOTATION)));
  }

  private double getCommonMetricErrorCount() {
    return TestMetricsUtil.getTotalCounter(
        EXT,
        METRIC_NAME + SUFFIX_ERROR,
        Collections.singleton(Tag.of(TAG_API_NAME, API_ANNOTATION)));
  }

  private int sendRequest(String principalName) {
    try (Response response =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/%s/%s", EXT.getLocalPort(), ENDPOINT, principalName))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .get()) {
      return response.getStatus();
    }
  }

  private void sendSuccessfulRequest() {
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), sendRequest("snowman"));
  }

  private void sendFailingRequest() {
    Assertions.assertNotEquals(
        Response.Status.NOT_FOUND.getStatusCode(), sendRequest("notarealprincipal"));
  }
}

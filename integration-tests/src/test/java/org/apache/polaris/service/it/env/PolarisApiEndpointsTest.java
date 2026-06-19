/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.it.env;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Unit tests for PolarisApiEndpoints */
public class PolarisApiEndpointsTest {

  @ParameterizedTest
  @CsvSource({
    "https://example.com          , https://example.com/api/catalog",
    "https://example.com/         , https://example.com/api/catalog",
    "https://example.com/polaris  , https://example.com/polaris/api/catalog",
    "https://example.com/polaris/ , https://example.com/polaris/api/catalog"
  })
  void testCatalogApiEndpoint(URI baseUri, URI expectedUri) {
    PolarisApiEndpoints endpoints =
        new PolarisApiEndpoints(
            baseUri, URI.create("https://example.com/irrelevant"), "realm1", Map.of());
    assertThat(endpoints.catalogApiEndpoint()).isEqualTo(expectedUri);
  }

  @ParameterizedTest
  @CsvSource({
    "https://example.com          , https://example.com/api/management",
    "https://example.com/         , https://example.com/api/management",
    "https://example.com/polaris  , https://example.com/polaris/api/management",
    "https://example.com/polaris/ , https://example.com/polaris/api/management"
  })
  void testManagementApiEndpoint(URI baseUri, URI expectedUri) {
    PolarisApiEndpoints endpoints =
        new PolarisApiEndpoints(
            baseUri, URI.create("https://example.com/irrelevant"), "realm1", Map.of());
    assertThat(endpoints.managementApiEndpoint()).isEqualTo(expectedUri);
  }

  @ParameterizedTest
  @CsvSource({
    "https://example.com          , https://example.com/metrics",
    "https://example.com/         , https://example.com/metrics",
    "https://example.com/polaris  , https://example.com/polaris/metrics",
    "https://example.com/polaris/ , https://example.com/polaris/metrics"
  })
  void testMetricsApiEndpoint(URI baseUri, URI expectedUri) {
    PolarisApiEndpoints endpoints =
        new PolarisApiEndpoints(
            URI.create("https://example.com/irrelevant"), baseUri, "realm1", Map.of());
    assertThat(endpoints.metricsApiEndpoint()).isEqualTo(expectedUri);
  }

  @Test
  void testMetricsApiEndpointUnavailable() {
    PolarisApiEndpoints endpoints =
        new PolarisApiEndpoints(
            URI.create("https://example.com/irrelevant"), null, "realm1", Map.of());
    assertThatThrownBy(endpoints::metricsApiEndpoint)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Management URI is not available");
  }

  @Test
  void testRealmId() {
    PolarisApiEndpoints endpoints =
        new PolarisApiEndpoints(
            URI.create("https://example.com/irrelevant"), null, "realm1", Map.of());
    assertThat(endpoints.realmId()).isEqualTo("realm1");
  }

  @Test
  void testExtraHeaders() {
    PolarisApiEndpoints endpoints =
        new PolarisApiEndpoints(
            URI.create("https://example.com/irrelevant"),
            null,
            "realm1",
            Map.of("key1", "value1", "key2", "value2"));
    assertThat(endpoints.extraHeaders())
        .containsOnly(Map.entry("key1", "value1"), Map.entry("key2", "value2"));
  }

  @Test
  void testExtraHeadersPrefix() {
    PolarisApiEndpoints endpoints =
        new PolarisApiEndpoints(
            URI.create("https://example.com/irrelevant"),
            null,
            "realm1",
            Map.of("key1", "value1", "key2", "value2"));
    assertThat(endpoints.extraHeaders("prefix."))
        .containsOnly(Map.entry("prefix.key1", "value1"), Map.entry("prefix.key2", "value2"));
  }
}

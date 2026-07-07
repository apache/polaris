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

import java.net.URI;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class PlatformApiEndpointsTest {

  @ParameterizedTest
  @CsvSource({
    "https://example.com          , https://example.com/metrics",
    "https://example.com/         , https://example.com/metrics",
    "https://example.com/polaris  , https://example.com/polaris/metrics",
    "https://example.com/polaris/ , https://example.com/polaris/metrics"
  })
  void testQuarkusMetricsApiEndpoint(URI baseUri, URI expectedUri) {
    PlatformApiEndpoints endpoints = new PlatformApiEndpoints(baseUri);
    assertThat(endpoints.metricsApiEndpoint()).isEqualTo(expectedUri);
  }

  @ParameterizedTest
  @CsvSource({
    "https://example.com          , https://example.com/health",
    "https://example.com/         , https://example.com/health",
    "https://example.com/polaris  , https://example.com/polaris/health",
    "https://example.com/polaris/ , https://example.com/polaris/health"
  })
  void testQuarkusHealthApiEndpoint(URI baseUri, URI expectedUri) {
    PlatformApiEndpoints endpoints = new PlatformApiEndpoints(baseUri);
    assertThat(endpoints.healthApiEndpoint()).isEqualTo(expectedUri);
  }
}

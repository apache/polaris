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
package org.apache.polaris.service.catalog.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.rest.RESTUtil;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify RESTUtil.merge() behavior that the federated catalog factories depend on.
 *
 * <p>The factories use RESTUtil.merge(catalogProperties, connectionConfigProperties) where
 * connectionConfigProperties (the second argument) takes precedence over catalogProperties.
 */
class IcebergRESTExternalCatalogFactoryTest {

  @Test
  void testRESTUtilMergeUpdatesOverrideTarget() {
    // This test documents the behavior we depend on: updates (second arg) override target
    Map<String, String> catalogProperties =
        Map.of("uri", "https://malicious.example.com", "proxy", "proxy.example.com");
    Map<String, String> connectionConfigProperties = Map.of("uri", "https://correct.example.com");

    Map<String, String> merged = RESTUtil.merge(catalogProperties, connectionConfigProperties);

    // Connection config properties should override catalog properties
    assertThat(merged)
        .containsEntry("uri", "https://correct.example.com")
        .containsEntry("proxy", "proxy.example.com");
  }

  @Test
  void testRESTUtilMergePreservesCatalogProperties() {
    // Catalog properties that don't conflict should be preserved
    Map<String, String> catalogProperties =
        Map.of(
            "rest.client.proxy.hostname", "proxy.example.com",
            "rest.client.proxy.port", "8080",
            "rest.client.connection-timeout-ms", "5000");
    Map<String, String> connectionConfigProperties = Map.of("uri", "https://example.com/catalog");

    Map<String, String> merged = RESTUtil.merge(catalogProperties, connectionConfigProperties);

    assertThat(merged)
        .containsEntry("rest.client.proxy.hostname", "proxy.example.com")
        .containsEntry("rest.client.proxy.port", "8080")
        .containsEntry("rest.client.connection-timeout-ms", "5000")
        .containsEntry("uri", "https://example.com/catalog");
  }

  @Test
  void testRESTUtilMergeWithEmptyTarget() {
    Map<String, String> connectionConfigProperties = Map.of("uri", "https://example.com/catalog");

    Map<String, String> merged = RESTUtil.merge(Map.of(), connectionConfigProperties);

    assertThat(merged).containsEntry("uri", "https://example.com/catalog");
  }
}

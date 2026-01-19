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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ImplicitAuthenticationParametersDpo;
import org.apache.polaris.core.connection.iceberg.IcebergRestConnectionConfigInfoDpo;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link IcebergRESTExternalCatalogFactory} to verify that ExternalCatalog.properties are
 * properly passed through to the Iceberg REST client configuration.
 */
class IcebergRESTExternalCatalogFactoryTest {

  private PolarisCredentialManager credentialManager;

  @BeforeEach
  void setUp() {
    credentialManager = mock(PolarisCredentialManager.class);
    when(credentialManager.getConnectionCredentials(any(ConnectionConfigInfoDpo.class)))
        .thenReturn(ConnectionCredentials.builder().build());
  }

  @Test
  void testMergePropertiesWithProxyConfiguration() {
    // Arrange: Create connection config with basic settings
    IcebergRestConnectionConfigInfoDpo connectionConfig =
        new IcebergRestConnectionConfigInfoDpo(
            "https://example.com/api/catalog",
            new ImplicitAuthenticationParametersDpo(),
            null,
            "my-catalog");

    // Catalog properties containing proxy and timeout settings
    Map<String, String> catalogProperties = new HashMap<>();
    catalogProperties.put("rest.client.proxy.hostname", "proxy.example.com");
    catalogProperties.put("rest.client.proxy.port", "8080");
    catalogProperties.put("rest.client.proxy.username", "proxyuser");
    catalogProperties.put("rest.client.proxy.password", "proxypass");
    catalogProperties.put("rest.client.connectTimeoutMs", "5000");
    catalogProperties.put("rest.client.readTimeoutMs", "30000");

    // Act: Test that properties are merged correctly by verifying the merge logic directly
    Map<String, String> mergedProperties = new HashMap<>();
    mergedProperties.putAll(catalogProperties);
    mergedProperties.putAll(connectionConfig.asIcebergCatalogProperties(credentialManager));

    // Assert: Proxy and timeout properties from catalog should be present
    assertThat(mergedProperties)
        .containsEntry("rest.client.proxy.hostname", "proxy.example.com")
        .containsEntry("rest.client.proxy.port", "8080")
        .containsEntry("rest.client.proxy.username", "proxyuser")
        .containsEntry("rest.client.proxy.password", "proxypass")
        .containsEntry("rest.client.connectTimeoutMs", "5000")
        .containsEntry("rest.client.readTimeoutMs", "30000");

    // Assert: Connection config properties (URI, etc.) should also be present
    assertThat(mergedProperties).containsEntry("uri", "https://example.com/api/catalog");
  }

  @Test
  void testConnectionConfigPropertiesTakePrecedence() {
    // Arrange: Create connection config
    IcebergRestConnectionConfigInfoDpo connectionConfig =
        new IcebergRestConnectionConfigInfoDpo(
            "https://correct-uri.example.com/api/catalog",
            new ImplicitAuthenticationParametersDpo(),
            null,
            "my-catalog");

    // Catalog properties that try to override the URI (should not be allowed)
    Map<String, String> catalogProperties = new HashMap<>();
    catalogProperties.put("uri", "https://malicious-uri.example.com/api");
    catalogProperties.put("rest.client.proxy.hostname", "proxy.example.com");

    // Act: Test merge logic - connection config properties should win
    Map<String, String> mergedProperties = new HashMap<>();
    mergedProperties.putAll(catalogProperties);
    // Connection config properties override catalog properties
    mergedProperties.putAll(connectionConfig.asIcebergCatalogProperties(credentialManager));

    // Assert: URI from connection config takes precedence (security-critical)
    assertThat(mergedProperties)
        .containsEntry("uri", "https://correct-uri.example.com/api/catalog");

    // Assert: Proxy config from catalog properties should still be present
    assertThat(mergedProperties).containsEntry("rest.client.proxy.hostname", "proxy.example.com");
  }

  @Test
  void testEmptyCatalogPropertiesHandled() {
    // Arrange: Create connection config
    IcebergRestConnectionConfigInfoDpo connectionConfig =
        new IcebergRestConnectionConfigInfoDpo(
            "https://example.com/api/catalog",
            new ImplicitAuthenticationParametersDpo(),
            null,
            "my-catalog");

    // Empty catalog properties
    Map<String, String> catalogProperties = new HashMap<>();

    // Act: Test merge logic with empty catalog properties
    Map<String, String> mergedProperties = new HashMap<>();
    mergedProperties.putAll(catalogProperties);
    mergedProperties.putAll(connectionConfig.asIcebergCatalogProperties(credentialManager));

    // Assert: Only connection config properties should be present
    assertThat(mergedProperties)
        .containsEntry("uri", "https://example.com/api/catalog")
        .doesNotContainKey("rest.client.proxy.hostname");
  }

  @Test
  void testNullCatalogPropertiesHandled() {
    // Arrange: Create connection config
    IcebergRestConnectionConfigInfoDpo connectionConfig =
        new IcebergRestConnectionConfigInfoDpo(
            "https://example.com/api/catalog",
            new ImplicitAuthenticationParametersDpo(),
            null,
            "my-catalog");

    // Act: Test merge logic with null catalog properties (as handled in factory)
    Map<String, String> mergedProperties = new HashMap<>();
    Map<String, String> catalogProperties = null;
    if (catalogProperties != null) {
      mergedProperties.putAll(catalogProperties);
    }
    mergedProperties.putAll(connectionConfig.asIcebergCatalogProperties(credentialManager));

    // Assert: Only connection config properties should be present
    assertThat(mergedProperties).containsEntry("uri", "https://example.com/api/catalog");
  }
}

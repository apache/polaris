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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.sun.net.httpserver.Headers;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.polaris.core.connection.ImplicitAuthenticationParametersDpo;
import org.apache.polaris.core.connection.iceberg.IcebergRestConnectionConfigInfoDpo;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.apache.polaris.service.distcache.HttpTestServer;
import org.junit.jupiter.api.Test;

class IcebergRESTFederatedCatalogFactoryTest {

  @Test
  void headerPropertiesAreSentOnEveryRemoteRequest() throws IOException {
    Map<String, Headers> requestHeadersByPath = new ConcurrentHashMap<>();
    try (HttpTestServer server =
        new HttpTestServer(
            "/",
            exchange -> {
              String path = exchange.getRequestURI().getPath();
              requestHeadersByPath.put(path, exchange.getRequestHeaders());
              String json =
                  path.endsWith("/namespaces")
                      ? "{\"namespaces\":[]}"
                      : "{\"defaults\":{},\"overrides\":{}}";
              byte[] body = json.getBytes(StandardCharsets.UTF_8);
              exchange.getResponseHeaders().add("Content-Type", "application/json");
              exchange.sendResponseHeaders(200, body.length);
              try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
              }
            })) {
      IcebergRestConnectionConfigInfoDpo connectionConfig =
          new IcebergRestConnectionConfigInfoDpo(
              server.getUri().toString(),
              new ImplicitAuthenticationParametersDpo(),
              null,
              "remote-catalog",
              Map.of("header.x-goog-user-project", "test-project"));
      PolarisCredentialManager credentialManager = mock(PolarisCredentialManager.class);
      when(credentialManager.getConnectionCredentials(connectionConfig))
          .thenReturn(ConnectionCredentials.EMPTY);

      Catalog catalog =
          new IcebergRESTFederatedCatalogFactory()
              .createCatalog(connectionConfig, credentialManager, Map.of());
      ((SupportsNamespaces) catalog).listNamespaces();

      // The config request carries the headers regardless (RESTSessionCatalog#fetchConfig passes
      // them explicitly); the requests after initialization depend on the HTTP client being built
      // with them.
      assertThat(requestHeadersByPath).containsKeys("/v1/config", "/v1/namespaces");
      assertThat(requestHeadersByPath.get("/v1/config").getFirst("x-goog-user-project"))
          .isEqualTo("test-project");
      assertThat(requestHeadersByPath.get("/v1/namespaces").getFirst("x-goog-user-project"))
          .isEqualTo("test-project");
    }
  }
}

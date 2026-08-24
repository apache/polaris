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
package org.apache.polaris.extension.auth.opa;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.extension.auth.opa.token.FileBearerTokenProvider;
import org.apache.polaris.nosql.async.java.JavaPoolAsyncExec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class OpaPolarisAuthorizerFactoryTest {

  @TempDir Path tempDir;

  @Test
  public void testFactoryWithStaticTokenConfiguration() {
    // Build configuration for static token
    OpaAuthorizationConfig opaConfig =
        ImmutableOpaAuthorizationConfig.builder()
            .policyUri(URI.create("http://localhost:8181/v1/data/polaris/authz/allow"))
            .auth(
                ImmutableAuthenticationConfig.builder()
                    .type(OpaAuthorizationConfig.AuthenticationType.BEARER)
                    .bearer(
                        ImmutableBearerTokenConfig.builder()
                            .staticToken(
                                ImmutableStaticTokenConfig.builder()
                                    .value("static-token-value")
                                    .build())
                            .build())
                    .build())
            .http(
                ImmutableHttpConfig.builder()
                    .timeout(Duration.ofSeconds(2))
                    .verifySsl(true)
                    .build())
            .build();

    try (JavaPoolAsyncExec asyncExec = new JavaPoolAsyncExec()) {
      RealmContext realmContext = () -> "test-realm";
      OpaPolarisAuthorizerFactory factory =
          new OpaPolarisAuthorizerFactory(
              opaConfig, Clock.systemUTC(), asyncExec, () -> null, realmContext);

      // Create authorizer
      RealmConfig realmConfig = mock(RealmConfig.class);
      OpaPolarisAuthorizer authorizer = (OpaPolarisAuthorizer) factory.create(realmConfig);

      assertThat(authorizer).isNotNull();
    }
  }

  @Test
  public void testFactoryWithFileBasedTokenConfiguration() throws IOException {
    // Create a temporary token file
    Path tokenFile = tempDir.resolve("bearer-token.txt");
    String tokenValue = "file-based-token-value";
    Files.writeString(tokenFile, tokenValue);

    // Build configuration for file-based token
    OpaAuthorizationConfig opaConfig =
        ImmutableOpaAuthorizationConfig.builder()
            .policyUri(URI.create("http://localhost:8181/v1/data/polaris/authz/allow"))
            .auth(
                ImmutableAuthenticationConfig.builder()
                    .type(OpaAuthorizationConfig.AuthenticationType.BEARER)
                    .bearer(
                        ImmutableBearerTokenConfig.builder()
                            .fileBased(
                                ImmutableFileBasedConfig.builder()
                                    .path(tokenFile)
                                    .refreshInterval(Duration.ofMinutes(5))
                                    .jwtExpirationRefresh(true)
                                    .jwtExpirationBuffer(Duration.ofMinutes(1))
                                    .build())
                            .build())
                    .build())
            .http(
                ImmutableHttpConfig.builder()
                    .timeout(Duration.ofSeconds(2))
                    .verifySsl(true)
                    .build())
            .build();

    try (JavaPoolAsyncExec asyncExec = new JavaPoolAsyncExec()) {
      RealmContext realmContext = () -> "test-realm";
      OpaPolarisAuthorizerFactory factory =
          new OpaPolarisAuthorizerFactory(
              opaConfig, Clock.systemUTC(), asyncExec, () -> null, realmContext);

      // Create authorizer
      RealmConfig realmConfig = mock(RealmConfig.class);
      OpaPolarisAuthorizer authorizer = (OpaPolarisAuthorizer) factory.create(realmConfig);

      assertThat(authorizer).isNotNull();

      // Also verify that the token provider actually reads from the file
      try (FileBearerTokenProvider provider =
          new FileBearerTokenProvider(
              tokenFile,
              Duration.ofMinutes(5),
              true,
              Duration.ofMinutes(1),
              Duration.ofSeconds(10),
              Duration.ofSeconds(1),
              asyncExec,
              Clock.systemUTC()::instant)) {

        String actualToken = provider.getToken();
        assertThat(actualToken).isEqualTo(tokenValue);
      }
    }
  }

  @Test
  public void testFactoryWithNoTokenConfiguration() {
    // Build configuration with no authentication
    OpaAuthorizationConfig opaConfig =
        ImmutableOpaAuthorizationConfig.builder()
            .policyUri(URI.create("http://localhost:8181/v1/data/polaris/authz/allow"))
            .auth(
                ImmutableAuthenticationConfig.builder()
                    .type(OpaAuthorizationConfig.AuthenticationType.NONE)
                    .build())
            .http(
                ImmutableHttpConfig.builder()
                    .timeout(Duration.ofSeconds(2))
                    .verifySsl(true)
                    .build())
            .build();

    try (JavaPoolAsyncExec asyncExec = new JavaPoolAsyncExec()) {
      RealmContext realmContext = () -> "test-realm";
      OpaPolarisAuthorizerFactory factory =
          new OpaPolarisAuthorizerFactory(
              opaConfig, Clock.systemUTC(), asyncExec, () -> null, realmContext);

      // Create authorizer
      RealmConfig realmConfig = mock(RealmConfig.class);
      OpaPolarisAuthorizer authorizer = (OpaPolarisAuthorizer) factory.create(realmConfig);

      assertThat(authorizer).isNotNull();
    }
  }

  @Test
  public void testFactoryPassesRealmToAuthorizerContext() throws Exception {
    final String[] capturedRequestBody = new String[1];

    HttpServer server = createServerWithRequestCapture(capturedRequestBody);
    try {
      OpaAuthorizationConfig opaConfig =
          ImmutableOpaAuthorizationConfig.builder()
              .policyUri(
                  URI.create(
                      "http://localhost:"
                          + server.getAddress().getPort()
                          + "/v1/data/polaris/allow"))
              .auth(
                  ImmutableAuthenticationConfig.builder()
                      .type(OpaAuthorizationConfig.AuthenticationType.NONE)
                      .build())
              .http(
                  ImmutableHttpConfig.builder()
                      .timeout(Duration.ofSeconds(2))
                      .verifySsl(true)
                      .build())
              .build();

      try (JavaPoolAsyncExec asyncExec = new JavaPoolAsyncExec()) {
        RealmContext realmContext = () -> "factory-realm";
        OpaPolarisAuthorizerFactory factory =
            new OpaPolarisAuthorizerFactory(
                opaConfig, Clock.systemUTC(), asyncExec, () -> null, realmContext);

        factory.initialize();
        OpaPolarisAuthorizer authorizer =
            (OpaPolarisAuthorizer) factory.create(mock(RealmConfig.class));

        PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("admin"));
        assertThatNoException()
            .isThrownBy(
                () ->
                    authorizer.authorizeOrThrow(
                        principal,
                        Set.of(),
                        PolarisAuthorizableOperation.GET_CATALOG,
                        (PolarisResolvedPathWrapper) null,
                        (PolarisResolvedPathWrapper) null));

        ObjectMapper mapper = JsonMapper.builder().build();
        JsonNode root = mapper.readTree(capturedRequestBody[0]);
        assertThat(root.path("input").path("context").get("realm").asText())
            .isEqualTo("factory-realm");
        factory.cleanup();
      }
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testFactoryUsesDistinctRealmValues() throws Exception {
    final String[] capturedRequestBody = new String[1];

    HttpServer server = createServerWithRequestCapture(capturedRequestBody);
    try {
      OpaAuthorizationConfig opaConfig =
          ImmutableOpaAuthorizationConfig.builder()
              .policyUri(
                  URI.create(
                      "http://localhost:"
                          + server.getAddress().getPort()
                          + "/v1/data/polaris/allow"))
              .auth(
                  ImmutableAuthenticationConfig.builder()
                      .type(OpaAuthorizationConfig.AuthenticationType.NONE)
                      .build())
              .http(
                  ImmutableHttpConfig.builder()
                      .timeout(Duration.ofSeconds(2))
                      .verifySsl(true)
                      .build())
              .build();

      try (JavaPoolAsyncExec asyncExec = new JavaPoolAsyncExec()) {
        RealmContext realmContext = () -> "realm-b";
        OpaPolarisAuthorizerFactory factory =
            new OpaPolarisAuthorizerFactory(
                opaConfig, Clock.systemUTC(), asyncExec, () -> null, realmContext);

        factory.initialize();
        OpaPolarisAuthorizer authorizer =
            (OpaPolarisAuthorizer) factory.create(mock(RealmConfig.class));

        PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("admin"));
        assertThatNoException()
            .isThrownBy(
                () ->
                    authorizer.authorizeOrThrow(
                        principal,
                        Set.of(),
                        PolarisAuthorizableOperation.GET_CATALOG,
                        (PolarisResolvedPathWrapper) null,
                        (PolarisResolvedPathWrapper) null));

        ObjectMapper mapper = JsonMapper.builder().build();
        JsonNode root = mapper.readTree(capturedRequestBody[0]);
        assertThat(root.path("input").path("context").get("realm").asText()).isEqualTo("realm-b");
        factory.cleanup();
      }
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testFactoryPassesResolvedRequestIdToAuthorizerContext() throws Exception {
    final String[] capturedRequestBody = new String[1];

    HttpServer server = createServerWithRequestCapture(capturedRequestBody);
    try {
      OpaAuthorizationConfig opaConfig =
          ImmutableOpaAuthorizationConfig.builder()
              .policyUri(
                  URI.create(
                      "http://localhost:"
                          + server.getAddress().getPort()
                          + "/v1/data/polaris/allow"))
              .auth(
                  ImmutableAuthenticationConfig.builder()
                      .type(OpaAuthorizationConfig.AuthenticationType.NONE)
                      .build())
              .http(
                  ImmutableHttpConfig.builder()
                      .timeout(Duration.ofSeconds(2))
                      .verifySsl(true)
                      .build())
              .build();

      try (JavaPoolAsyncExec asyncExec = new JavaPoolAsyncExec()) {
        RealmContext realmContext = () -> "test-realm";
        OpaPolarisAuthorizerFactory factory =
            new OpaPolarisAuthorizerFactory(
                opaConfig,
                Clock.systemUTC(),
                asyncExec,
                () -> "factory-test-request-id",
                realmContext);
        factory.initialize();

        RealmConfig realmConfig = mock(RealmConfig.class);
        OpaPolarisAuthorizer authorizer = (OpaPolarisAuthorizer) factory.create(realmConfig);

        PolarisPrincipal principal =
            PolarisPrincipal.of("eve", Map.of("department", "finance"), Set.of("auditor"));
        PolarisResolvedPathWrapper target = new PolarisResolvedPathWrapper(List.of());
        PolarisResolvedPathWrapper secondary = new PolarisResolvedPathWrapper(List.of());

        assertThatNoException()
            .isThrownBy(
                () ->
                    authorizer.authorizeOrThrow(
                        principal,
                        Set.<PolarisBaseEntity>of(),
                        PolarisAuthorizableOperation.LOAD_VIEW,
                        target,
                        secondary));

        JsonNode root = JsonMapper.builder().build().readTree(capturedRequestBody[0]);
        assertThat(root.at("/input/context/request_id").asText())
            .isEqualTo("factory-test-request-id");
      }
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testFactoryResolvesFreshRequestIdPerCreateCall() throws Exception {
    final String[] capturedRequestBody = new String[1];

    HttpServer server = createServerWithRequestCapture(capturedRequestBody);
    try {
      OpaAuthorizationConfig opaConfig =
          ImmutableOpaAuthorizationConfig.builder()
              .policyUri(
                  URI.create(
                      "http://localhost:"
                          + server.getAddress().getPort()
                          + "/v1/data/polaris/allow"))
              .auth(
                  ImmutableAuthenticationConfig.builder()
                      .type(OpaAuthorizationConfig.AuthenticationType.NONE)
                      .build())
              .http(
                  ImmutableHttpConfig.builder()
                      .timeout(Duration.ofSeconds(2))
                      .verifySsl(true)
                      .build())
              .build();

      // Simulates the CDI proxy resolving to a different value on each request, the way the real
      // RequestIdSupplier does via CurrentRequestManager.
      AtomicInteger requestCounter = new AtomicInteger();
      try (JavaPoolAsyncExec asyncExec = new JavaPoolAsyncExec()) {
        RealmContext realmContext = () -> "test-realm";
        OpaPolarisAuthorizerFactory factory =
            new OpaPolarisAuthorizerFactory(
                opaConfig,
                Clock.systemUTC(),
                asyncExec,
                () -> "request-" + requestCounter.incrementAndGet(),
                realmContext);

        factory.initialize();

        RealmConfig realmConfig = mock(RealmConfig.class);
        PolarisPrincipal principal =
            PolarisPrincipal.of("eve", Map.of("department", "finance"), Set.of("auditor"));
        PolarisResolvedPathWrapper target = new PolarisResolvedPathWrapper(List.of());
        PolarisResolvedPathWrapper secondary = new PolarisResolvedPathWrapper(List.of());

        // First "request": factory.create() is called fresh, as it would be for each incoming
        // HTTP request via the @RequestScoped PolarisAuthorizer producer.
        OpaPolarisAuthorizer firstAuthorizer = (OpaPolarisAuthorizer) factory.create(realmConfig);
        assertThatNoException()
            .isThrownBy(
                () ->
                    firstAuthorizer.authorizeOrThrow(
                        principal,
                        Set.<PolarisBaseEntity>of(),
                        PolarisAuthorizableOperation.LOAD_VIEW,
                        target,
                        secondary));
        JsonNode firstRoot = JsonMapper.builder().build().readTree(capturedRequestBody[0]);
        assertThat(firstRoot.at("/input/context/request_id").asText()).isEqualTo("request-1");

        // Second "request": a new authorizer is created against the same long-lived factory,
        // and must pick up a distinct, freshly-resolved request ID rather than reusing the first.
        OpaPolarisAuthorizer secondAuthorizer = (OpaPolarisAuthorizer) factory.create(realmConfig);
        assertThatNoException()
            .isThrownBy(
                () ->
                    secondAuthorizer.authorizeOrThrow(
                        principal,
                        Set.<PolarisBaseEntity>of(),
                        PolarisAuthorizableOperation.LOAD_VIEW,
                        target,
                        secondary));
        JsonNode secondRoot = JsonMapper.builder().build().readTree(capturedRequestBody[0]);
        assertThat(secondRoot.at("/input/context/request_id").asText()).isEqualTo("request-2");
      }
    } finally {
      server.stop(0);
    }
  }

  private HttpServer createServerWithRequestCapture(String[] capturedRequestBody)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(
        "/v1/data/polaris/allow",
        new HttpHandler() {
          @Override
          public void handle(HttpExchange exchange) throws IOException {
            byte[] requestBytes = exchange.getRequestBody().readAllBytes();
            capturedRequestBody[0] = new String(requestBytes, StandardCharsets.UTF_8);

            String response = "{\"result\":{\"allow\":true}}";
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
              os.write(response.getBytes(StandardCharsets.UTF_8));
            }
          }
        });
    server.start();
    return server;
  }
}

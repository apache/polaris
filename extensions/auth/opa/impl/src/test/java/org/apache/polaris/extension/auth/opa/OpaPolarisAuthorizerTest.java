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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.HttpEntities;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisSecurable;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.extension.auth.opa.token.BearerTokenProvider;
import org.apache.polaris.extension.auth.opa.token.StaticBearerTokenProvider;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for OpaPolarisAuthorizer including basic functionality and bearer token authentication
 */
public class OpaPolarisAuthorizerTest {

  @Test
  void testOpaInputJsonFormat() throws Exception {
    // Capture the request body for verification
    final String[] capturedRequestBody = new String[1];

    HttpServer server = createServerWithRequestCapture(capturedRequestBody);
    try {
      // Use the dynamically assigned port from the local server
      URI policyUri =
          URI.create(
              "http://localhost:" + server.getAddress().getPort() + "/v1/data/polaris/allow");
      OpaPolarisAuthorizer authorizer =
          new OpaPolarisAuthorizer(
              policyUri, HttpClients.createDefault(), JsonMapper.builder().build(), null);

      PolarisPrincipal principal =
          PolarisPrincipal.of("eve", Map.of("department", "finance"), Set.of("auditor"));

      PolarisSecurable target =
          PolarisSecurable.of(PolarisEntityType.TABLE_LIKE, List.of("ns", "table"));
      PolarisSecurable secondary = PolarisSecurable.of(PolarisEntityType.NAMESPACE, List.of("ns"));
      AuthorizationRequest request =
          AuthorizationRequest.of(
              principal,
              PolarisAuthorizableOperation.LOAD_VIEW,
              List.of(target),
              List.of(secondary));

      assertThatNoException().isThrownBy(() -> authorizer.authorize(mockState(), request));

      // Parse and verify JSON structure from captured request
      ObjectMapper mapper = JsonMapper.builder().build();
      JsonNode root = mapper.readTree(capturedRequestBody[0]);
      assertThat(root.has("input")).as("Root should have 'input' field").isTrue();
      var input = root.get("input");
      assertThat(input.has("actor")).as("Input should have 'actor' field").isTrue();
      assertThat(input.has("action")).as("Input should have 'action' field").isTrue();
      assertThat(input.has("resource")).as("Input should have 'resource' field").isTrue();
      assertThat(input.has("context")).as("Input should have 'context' field").isTrue();
    } finally {
      server.stop(0);
    }
  }

  @Test
  void testOpaRequestJsonWithHierarchicalResource() throws Exception {
    // Capture the request body for verification
    final String[] capturedRequestBody = new String[1];

    HttpServer server = createServerWithRequestCapture(capturedRequestBody);
    try {
      URI policyUri =
          URI.create(
              "http://localhost:" + server.getAddress().getPort() + "/v1/data/polaris/allow");
      OpaPolarisAuthorizer authorizer =
          new OpaPolarisAuthorizer(
              policyUri, HttpClients.createDefault(), JsonMapper.builder().build(), null);

      // Set up a realistic principal
      PolarisPrincipal principal =
          PolarisPrincipal.of(
              "alice",
              Map.of("department", "analytics", "level", "senior"),
              Set.of("data_engineer", "analyst"));

      PolarisSecurable tableSecurable =
          PolarisSecurable.of(
              PolarisEntityType.TABLE_LIKE, List.of("sales_data", "customer_orders"));
      AuthorizationRequest request =
          AuthorizationRequest.of(
              principal, PolarisAuthorizableOperation.LOAD_TABLE, List.of(tableSecurable), null);

      assertThatNoException().isThrownBy(() -> authorizer.authorize(mockState(), request));

      // Parse and verify the complete JSON structure
      ObjectMapper mapper = JsonMapper.builder().build();
      JsonNode root = mapper.readTree(capturedRequestBody[0]);

      // Verify top-level structure
      assertThat(root.has("input")).as("Root should have 'input' field").isTrue();
      var input = root.get("input");
      assertThat(input.has("actor")).as("Input should have 'actor' field").isTrue();
      assertThat(input.has("action")).as("Input should have 'action' field").isTrue();
      assertThat(input.has("resource")).as("Input should have 'resource' field").isTrue();
      assertThat(input.has("context")).as("Input should have 'context' field").isTrue();

      // Verify actor details
      var actor = input.get("actor");
      assertThat(actor.has("principal")).as("Actor should have 'principal' field").isTrue();
      assertThat(actor.get("principal").asText()).isEqualTo("alice");
      assertThat(actor.has("roles")).as("Actor should have 'roles' field").isTrue();
      assertThat(actor.get("roles").isArray()).as("Roles should be an array").isTrue();
      assertThat(actor.get("roles").size()).isEqualTo(2);

      // Verify action
      var action = input.get("action");
      assertThat(action.asText()).isEqualTo("LOAD_TABLE");

      // Verify resource structure - this is the key part for hierarchical resources
      var resource = input.get("resource");
      assertThat(resource.has("targets")).as("Resource should have 'targets' field").isTrue();

      var targets = resource.get("targets");
      assertThat(targets.isArray()).as("Targets should be an array").isTrue();
      assertThat(targets.size()).as("Should have exactly one target").isEqualTo(1);

      var target = targets.get(0);
      // Verify the target entity (table) details
      assertThat(target.isObject()).as("Target should be an object").isTrue();
      assertThat(target.has("type")).as("Target should have 'type' field").isTrue();
      assertThat(target.get("type").asText())
          .as("Target type should be TABLE_LIKE")
          .isEqualTo("TABLE_LIKE");
      assertThat(target.has("name")).as("Target should have 'name' field").isTrue();
      assertThat(target.get("name").asText())
          .as("Target name should be customer_orders")
          .isEqualTo("customer_orders");

      // Verify the hierarchical parents array
      assertThat(target.has("parents")).as("Target should have 'parents' field").isTrue();
      var parents = target.get("parents");
      assertThat(parents.isArray()).as("Parents should be an array").isTrue();
      assertThat(parents.size()).as("Should have 1 parent (namespace)").isEqualTo(1);

      // Verify namespace parent
      var namespaceParent = parents.get(0);
      assertThat(namespaceParent.get("type").asText())
          .as("Second parent should be namespace")
          .isEqualTo("NAMESPACE");
      assertThat(namespaceParent.get("name").asText())
          .as("Namespace name should be sales_data")
          .isEqualTo("sales_data");

      // Secondaries field should be omitted when empty (NON_EMPTY serialization)
      assertThat(resource.has("secondaries"))
          .as("Secondaries should be omitted when empty")
          .isFalse();
    } finally {
      server.stop(0);
    }
  }

  @Test
  void testOpaRequestJsonWithMultiLevelNamespace() throws Exception {
    // Capture the request body for verification
    final String[] capturedRequestBody = new String[1];

    HttpServer server = createServerWithRequestCapture(capturedRequestBody);
    try {
      URI policyUri =
          URI.create(
              "http://localhost:" + server.getAddress().getPort() + "/v1/data/polaris/allow");
      OpaPolarisAuthorizer authorizer =
          new OpaPolarisAuthorizer(
              policyUri, HttpClients.createDefault(), JsonMapper.builder().build(), null);

      // Set up a realistic principal
      PolarisPrincipal principal =
          PolarisPrincipal.of(
              "bob",
              Map.of("team", "ml", "project", "forecasting"),
              Set.of("data_scientist", "analyst"));

      // Create a multi-level namespace structure: catalog.department.team.table
      // Create catalog entity
      PolarisSecurable tableSecurable =
          PolarisSecurable.of(
              PolarisEntityType.TABLE_LIKE,
              List.of("engineering", "machine_learning", "feature_store"));
      AuthorizationRequest request =
          AuthorizationRequest.of(
              principal, PolarisAuthorizableOperation.LOAD_TABLE, List.of(tableSecurable), null);

      assertThatNoException().isThrownBy(() -> authorizer.authorize(mockState(), request));

      // Parse and verify the complete JSON structure
      ObjectMapper mapper = JsonMapper.builder().build();
      JsonNode root = mapper.readTree(capturedRequestBody[0]);

      // Verify top-level structure
      assertThat(root.has("input")).as("Root should have 'input' field").isTrue();
      var input = root.get("input");
      assertThat(input.has("actor")).as("Input should have 'actor' field").isTrue();
      assertThat(input.has("action")).as("Input should have 'action' field").isTrue();
      assertThat(input.has("resource")).as("Input should have 'resource' field").isTrue();
      assertThat(input.has("context")).as("Input should have 'context' field").isTrue();

      // Verify actor details
      var actor = input.get("actor");
      assertThat(actor.get("principal").asText()).isEqualTo("bob");
      assertThat(actor.get("roles").size()).isEqualTo(2);

      // Verify action
      var action = input.get("action");
      assertThat(action.asText()).isEqualTo("LOAD_TABLE");

      // Verify resource structure with multi-level namespace hierarchy
      var resource = input.get("resource");
      var targets = resource.get("targets");
      assertThat(targets.size()).as("Should have exactly one target").isEqualTo(1);

      var target = targets.get(0);
      // Verify the target entity (table) details
      assertThat(target.get("type").asText())
          .as("Target type should be TABLE_LIKE")
          .isEqualTo("TABLE_LIKE");
      assertThat(target.get("name").asText())
          .as("Target name should be feature_store")
          .isEqualTo("feature_store");

      // Verify the multi-level hierarchical parents array
      assertThat(target.has("parents")).as("Target should have 'parents' field").isTrue();
      var parents = target.get("parents");
      assertThat(parents.isArray()).as("Parents should be an array").isTrue();
      assertThat(parents.size()).as("Should have 2 parents (department, team)").isEqualTo(2);

      // Verify department namespace parent
      var departmentParent = parents.get(0);
      assertThat(departmentParent.get("type").asText())
          .as("Second parent should be namespace")
          .isEqualTo("NAMESPACE");
      assertThat(departmentParent.get("name").asText())
          .as("Department name should be engineering")
          .isEqualTo("engineering");

      // Verify team namespace parent
      var teamParent = parents.get(1);
      assertThat(teamParent.get("type").asText())
          .as("Third parent should be namespace")
          .isEqualTo("NAMESPACE");
      assertThat(teamParent.get("name").asText())
          .as("Team name should be machine_learning")
          .isEqualTo("machine_learning");

      // Secondaries field should be omitted when empty (NON_EMPTY serialization)
      assertThat(resource.has("secondaries"))
          .as("Secondaries should be omitted when empty")
          .isFalse();
    } finally {
      server.stop(0);
    }
  }

  @Test
  void testAuthorizeOrThrowWithEmptyTargetsAndSecondaries() throws Exception {
    HttpServer server = createServerWithAllowResponse();
    try {
      URI policyUri =
          URI.create(
              "http://localhost:" + server.getAddress().getPort() + "/v1/data/polaris/allow");
      OpaPolarisAuthorizer authorizer =
          new OpaPolarisAuthorizer(
              policyUri, HttpClients.createDefault(), JsonMapper.builder().build(), null);

      PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("admin"));

      AuthorizationRequest emptyRequest =
          AuthorizationRequest.of(
              principal, PolarisAuthorizableOperation.CREATE_CATALOG, List.of(), List.of());

      assertThatNoException().isThrownBy(() -> authorizer.authorize(mockState(), emptyRequest));

      // Test multiple targets
      List<PolarisSecurable> targets =
          List.of(
              PolarisSecurable.of(PolarisEntityType.NAMESPACE, List.of("ns1")),
              PolarisSecurable.of(PolarisEntityType.NAMESPACE, List.of("ns2")));
      AuthorizationRequest multiTargetRequest =
          AuthorizationRequest.of(
              principal, PolarisAuthorizableOperation.LOAD_VIEW, targets, List.of());

      assertThatNoException()
          .isThrownBy(() -> authorizer.authorize(mockState(), multiTargetRequest));
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testCreateWithHttpsAndBearerToken() {
    // Test that OpaPolarisAuthorizer can be created with HTTPS URLs and bearer tokens
    BearerTokenProvider tokenProvider = new StaticBearerTokenProvider("test-bearer-token");
    URI policyUri = URI.create("http://opa.example.com:8181/v1/data/polaris/allow");
    OpaPolarisAuthorizer authorizer =
        new OpaPolarisAuthorizer(
            policyUri, HttpClients.createDefault(), JsonMapper.builder().build(), tokenProvider);

    assertThat(authorizer).isNotNull();
  }

  @Test
  public void testBearerTokenIsAddedToHttpRequest() {
    URI policyUri = URI.create("http://opa.example.com:8181/v1/data/polaris/allow");
    HttpEntity mockEntity = HttpEntities.create("{\"result\":{\"allow\":true}}");
    @SuppressWarnings("resource")
    ClassicHttpResponse mockResponse = new BasicClassicHttpResponse(200);
    mockResponse.setEntity(mockEntity);

    BearerTokenProvider tokenProvider = new StaticBearerTokenProvider("test-bearer-token");
    OpaPolarisAuthorizer authorizer =
        new OpaPolarisAuthorizer(
            policyUri,
            mock(CloseableHttpClient.class),
            JsonMapper.builder().build(),
            tokenProvider) {
          @Override
          <T> T httpClientExecute(
              ClassicHttpRequest request, HttpClientResponseHandler<? extends T> responseHandler)
              throws HttpException, IOException {
            // Verify the Authorization header with static bearer token
            verifyAuthorizationHeader(request, "test-bearer-token");

            return responseHandler.handleResponse(mockResponse);
          }
        };

    PolarisPrincipal mockPrincipal =
        PolarisPrincipal.of("test-user", Map.of(), Collections.emptySet());

    PolarisAuthorizableOperation mockOperation = PolarisAuthorizableOperation.LOAD_TABLE;
    assertThatNoException()
        .isThrownBy(
            () -> {
              AuthorizationRequest request =
                  AuthorizationRequest.of(mockPrincipal, mockOperation, List.of(), List.of());
              authorizer.authorize(mockState(), request);
            });
  }

  @Test
  public void testBearerTokenFromBearerTokenProvider() {
    // Mock HTTP client and response
    HttpEntity mockEntity = HttpEntities.create("{\"result\":{\"allow\":true}}");
    @SuppressWarnings("resource")
    ClassicHttpResponse mockResponse = new BasicClassicHttpResponse(200);
    mockResponse.setEntity(mockEntity);

    // Create token provider that returns a dynamic token
    BearerTokenProvider tokenProvider = () -> "dynamic-token-12345";
    URI policyUri = URI.create("http://opa.example.com:8181/v1/data/polaris/allow");
    // Create authorizer with the token provider instead of static token
    OpaPolarisAuthorizer authorizer =
        new OpaPolarisAuthorizer(
            policyUri,
            mock(CloseableHttpClient.class),
            JsonMapper.builder().build(),
            tokenProvider) {
          @Override
          <T> T httpClientExecute(
              ClassicHttpRequest request, HttpClientResponseHandler<? extends T> responseHandler)
              throws HttpException, IOException {
            // Verify the Authorization header with bearer token from provider
            verifyAuthorizationHeader(request, "dynamic-token-12345");

            return responseHandler.handleResponse(mockResponse);
          }
        };

    // Create mock principal and entities
    PolarisPrincipal mockPrincipal =
        PolarisPrincipal.of("test-user", Map.of(), Collections.emptySet());

    PolarisAuthorizableOperation mockOperation = PolarisAuthorizableOperation.LOAD_TABLE;

    // Execute authorization (should not throw since we mocked allow=true)
    assertThatNoException()
        .isThrownBy(
            () -> {
              AuthorizationRequest request =
                  AuthorizationRequest.of(mockPrincipal, mockOperation, List.of(), List.of());
              authorizer.authorize(mockState(), request);
            });
  }

  /**
   * Helper method to create and start an HTTP server that captures request bodies.
   *
   * @param capturedRequestBody Array to store the captured request body
   * @return Started HttpServer instance
   */
  private HttpServer createServerWithRequestCapture(String[] capturedRequestBody)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(
        "/v1/data/polaris/allow",
        new HttpHandler() {
          @Override
          public void handle(HttpExchange exchange) throws IOException {
            // Capture request body
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

  /**
   * Helper method to create and start an HTTP server that returns a simple allow response.
   *
   * @return Started HttpServer instance
   */
  private HttpServer createServerWithAllowResponse() throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(
        "/v1/data/polaris/allow",
        new HttpHandler() {
          @Override
          public void handle(HttpExchange exchange) throws IOException {
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

  private static AuthorizationState mockState() {
    return mock(AuthorizationState.class);
  }

  /**
   * Helper method to capture and verify HTTP request Authorization header.
   *
   * @param capturedRequest The request issued to the HTTP client to verify against
   * @param expectedToken The expected bearer token value, or null if no Authorization header
   *     expected
   */
  private void verifyAuthorizationHeader(ClassicHttpRequest capturedRequest, String expectedToken) {
    // Capture the HTTP request to verify the bearer token header
    assertThat(capturedRequest).isInstanceOf(HttpPost.class);

    if (expectedToken != null) {
      // Verify the Authorization header is present and contains the expected token
      assertThat(capturedRequest.containsHeader("Authorization"))
          .as("Authorization header should be present when bearer token is provided")
          .isTrue();
      String authHeader = capturedRequest.getFirstHeader("Authorization").getValue();
      assertThat(authHeader)
          .as("Authorization header should contain the correct bearer token")
          .isEqualTo("Bearer " + expectedToken);
    } else {
      // Verify no Authorization header is present when token is null
      assertThat(capturedRequest.containsHeader("Authorization"))
          .as("Authorization header should not be present when token provider returns null")
          .isFalse();
    }
  }
}

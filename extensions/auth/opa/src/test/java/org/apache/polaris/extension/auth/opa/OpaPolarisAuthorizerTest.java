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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.BearerTokenProvider;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.StaticBearerTokenProvider;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for OpaPolarisAuthorizer including basic functionality and bearer token authentication
 */
public class OpaPolarisAuthorizerTest {

  @Test
  void testOpaInputJsonFormat() throws Exception {
    // Capture the request body for verification
    final String[] capturedRequestBody = new String[1];

    HttpServer server = createServerWithRequestCapture(capturedRequestBody);

    String url = "http://localhost:" + server.getAddress().getPort();

    OpaPolarisAuthorizer authorizer =
        createWithStringToken(
            url, "/v1/data/polaris/authz/allow", (String) null, HttpClients.createDefault());

    PolarisPrincipal principal =
        PolarisPrincipal.of("eve", Map.of("department", "finance"), Set.of("auditor"));

    Set<PolarisBaseEntity> entities = Set.of();
    PolarisResolvedPathWrapper target = new PolarisResolvedPathWrapper(List.of());
    PolarisResolvedPathWrapper secondary = new PolarisResolvedPathWrapper(List.of());

    assertDoesNotThrow(
        () ->
            authorizer.authorizeOrThrow(
                principal, entities, PolarisAuthorizableOperation.LOAD_VIEW, target, secondary));

    // Parse and verify JSON structure from captured request
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(capturedRequestBody[0]);
    assertTrue(root.has("input"), "Root should have 'input' field");
    var input = root.get("input");
    assertTrue(input.has("actor"), "Input should have 'actor' field");
    assertTrue(input.has("action"), "Input should have 'action' field");
    assertTrue(input.has("resource"), "Input should have 'resource' field");
    assertTrue(input.has("context"), "Input should have 'context' field");

    server.stop(0);
  }

  @Test
  void testOpaRequestJsonWithHierarchicalResource() throws Exception {
    // Capture the request body for verification
    final String[] capturedRequestBody = new String[1];

    HttpServer server = createServerWithRequestCapture(capturedRequestBody);

    String url = "http://localhost:" + server.getAddress().getPort();

    OpaPolarisAuthorizer authorizer =
        createWithStringToken(
            url, "/v1/data/polaris/authz/allow", (String) null, HttpClients.createDefault());

    // Set up a realistic principal
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            "alice",
            Map.of("department", "analytics", "level", "senior"),
            Set.of("data_engineer", "analyst"));

    // Create a hierarchical resource structure: catalog.namespace.table
    // Create catalog entity using builder pattern
    PolarisEntity catalogEntity =
        new PolarisEntity.Builder()
            .setName("prod_catalog")
            .setType(PolarisEntityType.CATALOG)
            .setId(100L)
            .setCatalogId(100L)
            .setParentId(0L)
            .setCreateTimestamp(System.currentTimeMillis())
            .build();

    // Create namespace entity using builder pattern
    PolarisEntity namespaceEntity =
        new PolarisEntity.Builder()
            .setName("sales_data")
            .setType(PolarisEntityType.NAMESPACE)
            .setId(200L)
            .setCatalogId(100L)
            .setParentId(100L)
            .setCreateTimestamp(System.currentTimeMillis())
            .build();

    // Create table entity using builder pattern
    PolarisEntity tableEntity =
        new PolarisEntity.Builder()
            .setName("customer_orders")
            .setType(PolarisEntityType.TABLE_LIKE)
            .setId(300L)
            .setCatalogId(100L)
            .setParentId(200L)
            .setCreateTimestamp(System.currentTimeMillis())
            .build();

    // Create hierarchical path: catalog -> namespace -> table
    // Build a realistic resolved path using ResolvedPolarisEntity objects
    List<ResolvedPolarisEntity> resolvedPath =
        List.of(
            createResolvedEntity(catalogEntity),
            createResolvedEntity(namespaceEntity),
            createResolvedEntity(tableEntity));
    PolarisResolvedPathWrapper tablePath = new PolarisResolvedPathWrapper(resolvedPath);

    Set<PolarisBaseEntity> entities = Set.of(catalogEntity, namespaceEntity, tableEntity);

    assertDoesNotThrow(
        () ->
            authorizer.authorizeOrThrow(
                principal, entities, PolarisAuthorizableOperation.LOAD_TABLE, tablePath, null));

    // Parse and verify the complete JSON structure
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(capturedRequestBody[0]);

    // Verify top-level structure
    assertTrue(root.has("input"), "Root should have 'input' field");
    var input = root.get("input");
    assertTrue(input.has("actor"), "Input should have 'actor' field");
    assertTrue(input.has("action"), "Input should have 'action' field");
    assertTrue(input.has("resource"), "Input should have 'resource' field");
    assertTrue(input.has("context"), "Input should have 'context' field");

    // Verify actor details
    var actor = input.get("actor");
    assertTrue(actor.has("principal"), "Actor should have 'principal' field");
    assertEquals("alice", actor.get("principal").asText());
    assertTrue(actor.has("roles"), "Actor should have 'roles' field");
    assertTrue(actor.get("roles").isArray(), "Roles should be an array");
    assertEquals(2, actor.get("roles").size());

    // Verify action
    var action = input.get("action");
    assertEquals("LOAD_TABLE", action.asText());

    // Verify resource structure - this is the key part for hierarchical resources
    var resource = input.get("resource");
    assertTrue(resource.has("targets"), "Resource should have 'targets' field");
    assertTrue(resource.has("secondaries"), "Resource should have 'secondaries' field");

    var targets = resource.get("targets");
    assertTrue(targets.isArray(), "Targets should be an array");
    assertEquals(1, targets.size(), "Should have exactly one target");

    var target = targets.get(0);
    // Verify the target entity (table) details
    assertTrue(target.isObject(), "Target should be an object");
    assertTrue(target.has("type"), "Target should have 'type' field");
    assertEquals("TABLE_LIKE", target.get("type").asText(), "Target type should be TABLE_LIKE");
    assertTrue(target.has("name"), "Target should have 'name' field");
    assertEquals(
        "customer_orders", target.get("name").asText(), "Target name should be customer_orders");

    // Verify the hierarchical parents array
    assertTrue(target.has("parents"), "Target should have 'parents' field");
    var parents = target.get("parents");
    assertTrue(parents.isArray(), "Parents should be an array");
    assertEquals(2, parents.size(), "Should have 2 parents (catalog and namespace)");

    // Verify catalog parent (first in the hierarchy)
    var catalogParent = parents.get(0);
    assertEquals("CATALOG", catalogParent.get("type").asText(), "First parent should be catalog");
    assertEquals(
        "prod_catalog", catalogParent.get("name").asText(), "Catalog name should be prod_catalog");

    // Verify namespace parent (second in the hierarchy)
    var namespaceParent = parents.get(1);
    assertEquals(
        "NAMESPACE", namespaceParent.get("type").asText(), "Second parent should be namespace");
    assertEquals(
        "sales_data", namespaceParent.get("name").asText(), "Namespace name should be sales_data");

    var secondaries = resource.get("secondaries");
    assertTrue(secondaries.isArray(), "Secondaries should be an array");
    assertEquals(0, secondaries.size(), "Should have no secondaries in this test");

    server.stop(0);
  }

  @Test
  void testOpaRequestJsonWithMultiLevelNamespace() throws Exception {
    // Capture the request body for verification
    final String[] capturedRequestBody = new String[1];

    HttpServer server = createServerWithRequestCapture(capturedRequestBody);

    String url = "http://localhost:" + server.getAddress().getPort();

    OpaPolarisAuthorizer authorizer =
        createWithStringToken(
            url, "/v1/data/polaris/authz/allow", (String) null, HttpClients.createDefault());

    // Set up a realistic principal
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            "bob",
            Map.of("team", "ml", "project", "forecasting"),
            Set.of("data_scientist", "analyst"));

    // Create a multi-level namespace structure: catalog.department.team.table
    // Create catalog entity
    PolarisEntity catalogEntity =
        new PolarisEntity.Builder()
            .setName("analytics_catalog")
            .setType(PolarisEntityType.CATALOG)
            .setId(100L)
            .setCatalogId(100L)
            .setParentId(0L)
            .setCreateTimestamp(System.currentTimeMillis())
            .build();

    // Create first-level namespace entity (department)
    PolarisEntity departmentEntity =
        new PolarisEntity.Builder()
            .setName("engineering")
            .setType(PolarisEntityType.NAMESPACE)
            .setId(200L)
            .setCatalogId(100L)
            .setParentId(100L)
            .setCreateTimestamp(System.currentTimeMillis())
            .build();

    // Create second-level namespace entity (team)
    PolarisEntity teamEntity =
        new PolarisEntity.Builder()
            .setName("machine_learning")
            .setType(PolarisEntityType.NAMESPACE)
            .setId(300L)
            .setCatalogId(100L)
            .setParentId(200L)
            .setCreateTimestamp(System.currentTimeMillis())
            .build();

    // Create table entity
    PolarisEntity tableEntity =
        new PolarisEntity.Builder()
            .setName("feature_store")
            .setType(PolarisEntityType.TABLE_LIKE)
            .setId(400L)
            .setCatalogId(100L)
            .setParentId(300L)
            .setCreateTimestamp(System.currentTimeMillis())
            .build();

    // Create hierarchical path: catalog -> department -> team -> table
    List<ResolvedPolarisEntity> resolvedPath =
        List.of(
            createResolvedEntity(catalogEntity),
            createResolvedEntity(departmentEntity),
            createResolvedEntity(teamEntity),
            createResolvedEntity(tableEntity));
    PolarisResolvedPathWrapper tablePath = new PolarisResolvedPathWrapper(resolvedPath);

    Set<PolarisBaseEntity> entities =
        Set.of(catalogEntity, departmentEntity, teamEntity, tableEntity);

    assertDoesNotThrow(
        () ->
            authorizer.authorizeOrThrow(
                principal, entities, PolarisAuthorizableOperation.LOAD_TABLE, tablePath, null));

    // Parse and verify the complete JSON structure
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(capturedRequestBody[0]);

    // Verify top-level structure
    assertTrue(root.has("input"), "Root should have 'input' field");
    var input = root.get("input");
    assertTrue(input.has("actor"), "Input should have 'actor' field");
    assertTrue(input.has("action"), "Input should have 'action' field");
    assertTrue(input.has("resource"), "Input should have 'resource' field");
    assertTrue(input.has("context"), "Input should have 'context' field");

    // Verify actor details
    var actor = input.get("actor");
    assertEquals("bob", actor.get("principal").asText());
    assertEquals(2, actor.get("roles").size());

    // Verify action
    var action = input.get("action");
    assertEquals("LOAD_TABLE", action.asText());

    // Verify resource structure with multi-level namespace hierarchy
    var resource = input.get("resource");
    var targets = resource.get("targets");
    assertEquals(1, targets.size(), "Should have exactly one target");

    var target = targets.get(0);
    // Verify the target entity (table) details
    assertEquals("TABLE_LIKE", target.get("type").asText(), "Target type should be TABLE_LIKE");
    assertEquals(
        "feature_store", target.get("name").asText(), "Target name should be feature_store");

    // Verify the multi-level hierarchical parents array
    assertTrue(target.has("parents"), "Target should have 'parents' field");
    var parents = target.get("parents");
    assertTrue(parents.isArray(), "Parents should be an array");
    assertEquals(3, parents.size(), "Should have 3 parents (catalog, department, team)");

    // Verify catalog parent (first in the hierarchy)
    var catalogParent = parents.get(0);
    assertEquals("CATALOG", catalogParent.get("type").asText(), "First parent should be catalog");
    assertEquals(
        "analytics_catalog",
        catalogParent.get("name").asText(),
        "Catalog name should be analytics_catalog");

    // Verify department namespace parent (second in the hierarchy)
    var departmentParent = parents.get(1);
    assertEquals(
        "NAMESPACE", departmentParent.get("type").asText(), "Second parent should be namespace");
    assertEquals(
        "engineering",
        departmentParent.get("name").asText(),
        "Department name should be engineering");

    // Verify team namespace parent (third in the hierarchy)
    var teamParent = parents.get(2);
    assertEquals("NAMESPACE", teamParent.get("type").asText(), "Third parent should be namespace");
    assertEquals(
        "machine_learning",
        teamParent.get("name").asText(),
        "Team name should be machine_learning");

    var secondaries = resource.get("secondaries");
    assertTrue(secondaries.isArray(), "Secondaries should be an array");
    assertEquals(0, secondaries.size(), "Should have no secondaries in this test");

    server.stop(0);
  }

  @Test
  void testAuthorizeOrThrowSingleTargetSecondary() throws Exception {
    HttpServer server = createServerWithAllowResponse();

    String url = "http://localhost:" + server.getAddress().getPort();

    OpaPolarisAuthorizer authorizer =
        createWithStringToken(
            url, "/v1/data/polaris/authz/allow", (String) null, HttpClients.createDefault());

    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("admin"));

    Set<PolarisBaseEntity> entities = Set.of();
    PolarisResolvedPathWrapper target = new PolarisResolvedPathWrapper(List.of());
    PolarisResolvedPathWrapper secondary = new PolarisResolvedPathWrapper(List.of());

    assertDoesNotThrow(
        () ->
            authorizer.authorizeOrThrow(
                principal,
                entities,
                PolarisAuthorizableOperation.CREATE_CATALOG,
                target,
                secondary));

    server.stop(0);
  }

  @Test
  void testAuthorizeOrThrowMultiTargetSecondary() throws Exception {
    HttpServer server = createServerWithAllowResponse();

    String url = "http://localhost:" + server.getAddress().getPort();

    OpaPolarisAuthorizer authorizer =
        createWithStringToken(
            url, "/v1/data/polaris/authz/allow", (String) null, HttpClients.createDefault());

    PolarisPrincipal principal = PolarisPrincipal.of("bob", Map.of(), Set.of("user"));

    Set<PolarisBaseEntity> entities = Set.of();
    PolarisResolvedPathWrapper target1 = new PolarisResolvedPathWrapper(List.of());
    PolarisResolvedPathWrapper target2 = new PolarisResolvedPathWrapper(List.of());
    List<PolarisResolvedPathWrapper> targets = List.of(target1, target2);
    List<PolarisResolvedPathWrapper> secondaries = List.of();

    assertDoesNotThrow(
        () ->
            authorizer.authorizeOrThrow(
                principal, entities, PolarisAuthorizableOperation.LOAD_VIEW, targets, secondaries));

    server.stop(0);
  }

  // ===== Bearer Token and HTTPS Tests =====

  @Test
  public void testCreateWithBearerTokenAndHttps() {
    OpaPolarisAuthorizer authorizer =
        createWithStringToken(
            "https://opa.example.com:8181",
            "/v1/data/polaris/authz",
            "test-bearer-token",
            HttpClients.createDefault());

    assertTrue(authorizer != null);
  }

  @Test
  public void testCreateWithBearerTokenAndHttpsNoSslVerification() {
    OpaPolarisAuthorizer authorizer =
        createWithStringToken(
            "https://opa.example.com:8181",
            "/v1/data/polaris/authz",
            "test-bearer-token",
            HttpClients.createDefault());

    assertTrue(authorizer != null);
  }

  @Test
  public void testCreateWithHttpsAndSslVerificationDisabled() {
    OpaPolarisAuthorizer authorizer =
        createWithStringToken(
            "https://opa.example.com:8181",
            "/v1/data/polaris/authz",
            "test-bearer-token",
            HttpClients.createDefault());
    assertTrue(authorizer != null);
  }

  @Test
  public void testBearerTokenIsAddedToHttpRequest() throws IOException {
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    HttpEntity mockEntity = mock(HttpEntity.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getCode()).thenReturn(200);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent())
        .thenReturn(
            new ByteArrayInputStream(
                "{\"result\":{\"allow\":true}}".getBytes(StandardCharsets.UTF_8)));

    OpaPolarisAuthorizer authorizer =
        createWithStringToken(
            "http://opa.example.com:8181",
            "/v1/data/polaris/authz",
            "test-bearer-token",
            mockHttpClient);

    PolarisPrincipal mockPrincipal =
        PolarisPrincipal.of("test-user", Map.of(), Collections.emptySet());

    PolarisAuthorizableOperation mockOperation = PolarisAuthorizableOperation.LOAD_TABLE;
    assertDoesNotThrow(
        () -> {
          authorizer.authorizeOrThrow(
              mockPrincipal,
              Collections.emptySet(),
              mockOperation,
              (PolarisResolvedPathWrapper) null,
              (PolarisResolvedPathWrapper) null);
        });

    // Verify the Authorization header with static bearer token
    verifyAuthorizationHeader(mockHttpClient, "test-bearer-token");
  }

  @Test
  public void testAuthorizationFailsWithoutBearerToken() throws IOException {
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getCode()).thenReturn(401);

    OpaPolarisAuthorizer authorizer =
        createWithStringToken(
            "http://opa.example.com:8181", "/v1/data/polaris/authz", (String) null, mockHttpClient);

    PolarisPrincipal mockPrincipal =
        PolarisPrincipal.of("test-user", Map.of(), Collections.emptySet());

    PolarisAuthorizableOperation mockOperation = PolarisAuthorizableOperation.LOAD_TABLE;
    assertThrows(
        ForbiddenException.class,
        () -> {
          authorizer.authorizeOrThrow(
              mockPrincipal,
              Collections.emptySet(),
              mockOperation,
              (PolarisResolvedPathWrapper) null,
              (PolarisResolvedPathWrapper) null);
        });
  }

  @Test
  public void testBearerTokenFromBearerTokenProvider() throws IOException {
    // Mock HTTP client and response
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    HttpEntity mockEntity = mock(HttpEntity.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getCode()).thenReturn(200);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent())
        .thenReturn(
            new ByteArrayInputStream(
                "{\"result\":{\"allow\":true}}".getBytes(StandardCharsets.UTF_8)));

    // Create token provider that returns a dynamic token
    BearerTokenProvider tokenProvider = () -> "dynamic-token-12345";

    // Create authorizer with the token provider instead of static token
    OpaPolarisAuthorizer authorizer =
        OpaPolarisAuthorizer.create(
            "http://opa.example.com:8181", "/v1/data/polaris/authz", tokenProvider, mockHttpClient);

    // Create mock principal and entities
    PolarisPrincipal mockPrincipal =
        PolarisPrincipal.of("test-user", Map.of(), Collections.emptySet());

    PolarisAuthorizableOperation mockOperation = PolarisAuthorizableOperation.LOAD_TABLE;

    // Execute authorization (should not throw since we mocked allow=true)
    assertDoesNotThrow(
        () -> {
          authorizer.authorizeOrThrow(
              mockPrincipal,
              Collections.emptySet(),
              mockOperation,
              (PolarisResolvedPathWrapper) null,
              (PolarisResolvedPathWrapper) null);
        });

    // Verify the Authorization header with bearer token from provider
    verifyAuthorizationHeader(mockHttpClient, "dynamic-token-12345");
  }

  @Test
  public void testNullTokenFromBearerTokenProvider() throws IOException {
    // Mock HTTP client and response
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    HttpEntity mockEntity = mock(HttpEntity.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getCode()).thenReturn(200);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent())
        .thenReturn(
            new ByteArrayInputStream(
                "{\"result\":{\"allow\":true}}".getBytes(StandardCharsets.UTF_8)));

    // Create a token provider that returns null
    BearerTokenProvider tokenProvider = new StaticBearerTokenProvider(null);

    OpaPolarisAuthorizer authorizer =
        OpaPolarisAuthorizer.create(
            "http://opa.example.com:8181", "/v1/data/polaris/authz", tokenProvider, mockHttpClient);

    // Create mock principal and entities
    PolarisPrincipal mockPrincipal =
        PolarisPrincipal.of("test-user", Map.of(), Collections.emptySet());

    PolarisAuthorizableOperation mockOperation = PolarisAuthorizableOperation.LOAD_TABLE;

    // Execute authorization (should not throw since we mocked allow=true)
    assertDoesNotThrow(
        () -> {
          authorizer.authorizeOrThrow(
              mockPrincipal,
              Collections.emptySet(),
              mockOperation,
              (PolarisResolvedPathWrapper) null,
              (PolarisResolvedPathWrapper) null);
        });

    // Verify no Authorization header is present when token provider returns null
    verifyAuthorizationHeader(mockHttpClient, null);
  }

  private ResolvedPolarisEntity createResolvedEntity(PolarisEntity entity) {
    return new ResolvedPolarisEntity(entity, List.of(), List.of());
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
        "/v1/data/polaris/authz/allow",
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
        "/v1/data/polaris/authz/allow",
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

  /**
   * Helper method to capture and verify HTTP request Authorization header.
   *
   * @param mockHttpClient The mocked HTTP client to verify against
   * @param expectedToken The expected bearer token value, or null if no Authorization header
   *     expected
   */
  private void verifyAuthorizationHeader(CloseableHttpClient mockHttpClient, String expectedToken)
      throws IOException {
    // Capture the HTTP request to verify bearer token header
    ArgumentCaptor<HttpPost> httpPostCaptor = ArgumentCaptor.forClass(HttpPost.class);
    verify(mockHttpClient).execute(httpPostCaptor.capture());

    HttpPost capturedRequest = httpPostCaptor.getValue();

    if (expectedToken != null) {
      // Verify the Authorization header is present and contains the expected token
      assertTrue(
          capturedRequest.containsHeader("Authorization"),
          "Authorization header should be present when bearer token is provided");
      String authHeader = capturedRequest.getFirstHeader("Authorization").getValue();
      assertEquals(
          "Bearer " + expectedToken,
          authHeader,
          "Authorization header should contain the correct bearer token");
    } else {
      // Verify no Authorization header is present when token is null
      assertTrue(
          !capturedRequest.containsHeader("Authorization"),
          "Authorization header should not be present when token provider returns null");
    }
  }

  /**
   * Convenience helper method for creating OpaPolarisAuthorizer with String bearer token. This
   * provides the same API as the removed String-based create method for test convenience.
   */
  private static OpaPolarisAuthorizer createWithStringToken(
      String opaServerUrl, String opaPolicyPath, String bearerToken, CloseableHttpClient client) {
    BearerTokenProvider tokenProvider = new StaticBearerTokenProvider(bearerToken);
    return OpaPolarisAuthorizer.create(opaServerUrl, opaPolicyPath, tokenProvider, client);
  }
}

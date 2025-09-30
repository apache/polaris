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
package org.apache.polaris.core.auth;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for OpaPolarisAuthorizer including basic functionality and bearer token authentication
 */
public class OpaPolarisAuthorizerTest {

  @Test
  void testOpaInputJsonFormat() throws Exception {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setBody("{\"result\":{\"allow\":true}}"));
    server.start();
    String url = server.url("/v1/data/polaris/authz/allow").toString();

    OpaPolarisAuthorizer authorizer =
        OpaPolarisAuthorizer.create(
            url.replace("/v1/data/polaris/authz/allow", ""),
            "/v1/data/polaris/authz/allow",
            (String) null,
            2000,
            true,
            null,
            null,
            null,
            null);

    PolarisPrincipal principal = Mockito.mock(PolarisPrincipal.class);
    Mockito.when(principal.getName()).thenReturn("eve");
    Mockito.when(principal.getRoles()).thenReturn(Set.of("auditor"));
    Mockito.when(principal.getProperties()).thenReturn(Map.of("department", "finance"));

    Set<PolarisBaseEntity> entities = Set.of();
    PolarisResolvedPathWrapper target = Mockito.mock(PolarisResolvedPathWrapper.class);
    PolarisResolvedPathWrapper secondary = Mockito.mock(PolarisResolvedPathWrapper.class);

    assertDoesNotThrow(
        () ->
            authorizer.authorizeOrThrow(
                principal, entities, PolarisAuthorizableOperation.LOAD_VIEW, target, secondary));

    // Get the request sent to the mock server
    var recordedRequest = server.takeRequest();
    String requestBody = recordedRequest.getBody().readUtf8();

    // Parse and verify JSON structure
    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(requestBody);
    assertTrue(root.has("input"), "Root should have 'input' field");
    var input = root.get("input");
    assertTrue(input.has("actor"), "Input should have 'actor' field");
    assertTrue(input.has("action"), "Input should have 'action' field");
    assertTrue(input.has("resource"), "Input should have 'resource' field");
    assertTrue(input.has("context"), "Input should have 'context' field");

    server.shutdown();
  }

  @Test
  void testAuthorizeOrThrowSingleTargetSecondary() throws Exception {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setBody("{\"result\":{\"allow\":true}}"));
    server.start();
    String url = server.url("/v1/data/polaris/authz/allow").toString();

    OpaPolarisAuthorizer authorizer =
        OpaPolarisAuthorizer.create(
            url.replace("/v1/data/polaris/authz/allow", ""),
            "/v1/data/polaris/authz/allow",
            (String) null,
            2000,
            true,
            null,
            null,
            null,
            null);

    PolarisPrincipal principal = Mockito.mock(PolarisPrincipal.class);
    Mockito.when(principal.getName()).thenReturn("alice");
    Mockito.when(principal.getRoles()).thenReturn(Set.of("admin"));
    Mockito.when(principal.getProperties()).thenReturn(Map.of());

    Set<PolarisBaseEntity> entities = Set.of();
    PolarisResolvedPathWrapper target = Mockito.mock(PolarisResolvedPathWrapper.class);
    PolarisResolvedPathWrapper secondary = Mockito.mock(PolarisResolvedPathWrapper.class);

    assertDoesNotThrow(
        () ->
            authorizer.authorizeOrThrow(
                principal,
                entities,
                PolarisAuthorizableOperation.CREATE_CATALOG,
                target,
                secondary));

    server.shutdown();
  }

  @Test
  void testAuthorizeOrThrowMultiTargetSecondary() throws Exception {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setBody("{\"result\":{\"allow\":true}}"));
    server.start();
    String url = server.url("/v1/data/polaris/authz/allow").toString();

    OpaPolarisAuthorizer authorizer =
        OpaPolarisAuthorizer.create(
            url.replace("/v1/data/polaris/authz/allow", ""),
            "/v1/data/polaris/authz/allow",
            (String) null,
            2000,
            true,
            null,
            null,
            null,
            null);

    PolarisPrincipal principal = Mockito.mock(PolarisPrincipal.class);
    Mockito.when(principal.getName()).thenReturn("bob");
    Mockito.when(principal.getRoles()).thenReturn(Set.of("user"));
    Mockito.when(principal.getProperties()).thenReturn(Map.of());

    Set<PolarisBaseEntity> entities = Set.of();
    PolarisResolvedPathWrapper target1 = Mockito.mock(PolarisResolvedPathWrapper.class);
    PolarisResolvedPathWrapper target2 = Mockito.mock(PolarisResolvedPathWrapper.class);
    List<PolarisResolvedPathWrapper> targets = List.of(target1, target2);
    List<PolarisResolvedPathWrapper> secondaries = List.of();

    assertDoesNotThrow(
        () ->
            authorizer.authorizeOrThrow(
                principal, entities, PolarisAuthorizableOperation.LOAD_VIEW, targets, secondaries));

    server.shutdown();
  }

  // ===== Bearer Token and HTTPS Tests =====

  @Test
  public void testCreateWithBearerTokenAndHttps() {
    OpaPolarisAuthorizer authorizer =
        OpaPolarisAuthorizer.create(
            "https://opa.example.com:8181",
            "/v1/data/polaris/authz",
            "test-bearer-token",
            2000,
            true,
            null,
            null,
            null,
            null);

    assertTrue(authorizer != null);
  }

  @Test
  public void testCreateWithBearerTokenAndHttpsNoSslVerification() {
    OpaPolarisAuthorizer authorizer =
        OpaPolarisAuthorizer.create(
            "https://opa.example.com:8181",
            "/v1/data/polaris/authz",
            "test-bearer-token",
            2000,
            false,
            null,
            null,
            null,
            null);

    assertTrue(authorizer != null);
  }

  @Test
  public void testCreateWithHttpsAndSslVerificationDisabled() {
    OpaPolarisAuthorizer authorizer =
        OpaPolarisAuthorizer.create(
            "https://opa.example.com:8181",
            "/v1/data/polaris/authz",
            "test-bearer-token",
            2000,
            false,
            null,
            null,
            null,
            null);
    assertTrue(authorizer != null);
  }

  @Test
  public void testBearerTokenIsAddedToHttpRequest() throws IOException {
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    HttpEntity mockEntity = mock(HttpEntity.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(200);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent())
        .thenReturn(
            new ByteArrayInputStream(
                "{\"result\":{\"allow\":true}}".getBytes(StandardCharsets.UTF_8)));

    OpaPolarisAuthorizer authorizer =
        OpaPolarisAuthorizer.create(
            "http://opa.example.com:8181",
            "/v1/data/polaris/authz",
            "test-bearer-token",
            2000,
            true,
            null,
            null,
            mockHttpClient,
            new ObjectMapper());

    PolarisPrincipal mockPrincipal = mock(PolarisPrincipal.class);
    when(mockPrincipal.getName()).thenReturn("test-user");
    when(mockPrincipal.getRoles()).thenReturn(Collections.emptySet());
    when(mockPrincipal.getProperties()).thenReturn(Map.of());

    PolarisAuthorizableOperation mockOperation = mock(PolarisAuthorizableOperation.class);
    when(mockOperation.name()).thenReturn("READ");
    assertDoesNotThrow(
        () -> {
          authorizer.authorizeOrThrow(
              mockPrincipal,
              Collections.emptySet(),
              mockOperation,
              (PolarisResolvedPathWrapper) null,
              (PolarisResolvedPathWrapper) null);
        });

    ArgumentCaptor<HttpPost> httpPostCaptor = ArgumentCaptor.forClass(HttpPost.class);
    verify(mockHttpClient).execute(httpPostCaptor.capture());

    HttpPost capturedRequest = httpPostCaptor.getValue();
    assertTrue(capturedRequest.containsHeader("Authorization"));
    String authHeader = capturedRequest.getFirstHeader("Authorization").getValue();
    assertTrue(
        authHeader.equals("Bearer test-bearer-token"),
        "Expected 'Bearer test-bearer-token' but got '" + authHeader + "'");
  }

  @Test
  public void testAuthorizationFailsWithoutBearerToken() throws IOException {
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(401);

    OpaPolarisAuthorizer authorizer =
        OpaPolarisAuthorizer.create(
            "http://opa.example.com:8181",
            "/v1/data/polaris/authz",
            (String) null,
            2000,
            true,
            null,
            null,
            mockHttpClient,
            new ObjectMapper());

    PolarisPrincipal mockPrincipal = mock(PolarisPrincipal.class);
    when(mockPrincipal.getName()).thenReturn("test-user");
    when(mockPrincipal.getRoles()).thenReturn(Collections.emptySet());
    when(mockPrincipal.getProperties()).thenReturn(Map.of());

    PolarisAuthorizableOperation mockOperation = mock(PolarisAuthorizableOperation.class);
    when(mockOperation.name()).thenReturn("READ");
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
  public void testBearerTokenFromTokenProvider() throws IOException {
    // Mock HTTP client and response
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    HttpEntity mockEntity = mock(HttpEntity.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(200);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent())
        .thenReturn(
            new ByteArrayInputStream(
                "{\"result\":{\"allow\":true}}".getBytes(StandardCharsets.UTF_8)));

    // Create a custom token provider
    TokenProvider tokenProvider = new StaticTokenProvider("custom-token-from-provider");

    OpaPolarisAuthorizer authorizer =
        OpaPolarisAuthorizer.create(
            "http://opa.example.com:8181",
            "/v1/data/polaris/authz",
            tokenProvider,
            2000,
            true,
            null,
            null,
            mockHttpClient,
            new ObjectMapper());

    // Create mock principal and entities
    PolarisPrincipal mockPrincipal = mock(PolarisPrincipal.class);
    when(mockPrincipal.getName()).thenReturn("test-user");
    when(mockPrincipal.getRoles()).thenReturn(Collections.emptySet());
    when(mockPrincipal.getProperties()).thenReturn(Map.of());

    PolarisAuthorizableOperation mockOperation = mock(PolarisAuthorizableOperation.class);
    when(mockOperation.name()).thenReturn("READ");

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

    // Capture the HTTP request to verify bearer token header
    ArgumentCaptor<HttpPost> httpPostCaptor = ArgumentCaptor.forClass(HttpPost.class);
    verify(mockHttpClient).execute(httpPostCaptor.capture());

    HttpPost capturedRequest = httpPostCaptor.getValue();

    // Verify the Authorization header with bearer token from provider
    assertTrue(capturedRequest.containsHeader("Authorization"));
    String authHeader = capturedRequest.getFirstHeader("Authorization").getValue();
    assertTrue(
        authHeader.equals("Bearer custom-token-from-provider"),
        "Expected 'Bearer custom-token-from-provider' but got '" + authHeader + "'");
  }

  @Test
  public void testNullTokenFromTokenProvider() throws IOException {
    // Mock HTTP client and response
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    HttpEntity mockEntity = mock(HttpEntity.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(200);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent())
        .thenReturn(
            new ByteArrayInputStream(
                "{\"result\":{\"allow\":true}}".getBytes(StandardCharsets.UTF_8)));

    // Create a token provider that returns null
    TokenProvider tokenProvider = new StaticTokenProvider(null);

    OpaPolarisAuthorizer authorizer =
        OpaPolarisAuthorizer.create(
            "http://opa.example.com:8181",
            "/v1/data/polaris/authz",
            tokenProvider,
            2000,
            true,
            null,
            null,
            mockHttpClient,
            new ObjectMapper());

    // Create mock principal and entities
    PolarisPrincipal mockPrincipal = mock(PolarisPrincipal.class);
    when(mockPrincipal.getName()).thenReturn("test-user");
    when(mockPrincipal.getRoles()).thenReturn(Collections.emptySet());
    when(mockPrincipal.getProperties()).thenReturn(Map.of());

    PolarisAuthorizableOperation mockOperation = mock(PolarisAuthorizableOperation.class);
    when(mockOperation.name()).thenReturn("READ");

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

    // Capture the HTTP request to verify no Authorization header is set
    ArgumentCaptor<HttpPost> httpPostCaptor = ArgumentCaptor.forClass(HttpPost.class);
    verify(mockHttpClient).execute(httpPostCaptor.capture());

    HttpPost capturedRequest = httpPostCaptor.getValue();

    // Verify no Authorization header is present when token provider returns null
    assertTrue(
        !capturedRequest.containsHeader("Authorization")
            || capturedRequest.getFirstHeader("Authorization") == null);
  }
}

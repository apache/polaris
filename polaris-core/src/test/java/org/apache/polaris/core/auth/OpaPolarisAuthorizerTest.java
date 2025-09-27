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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
            2000,
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
            2000,
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
            2000,
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
}

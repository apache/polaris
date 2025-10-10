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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.BearerTokenProvider;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;

/**
 * OPA-based implementation of {@link PolarisAuthorizer}.
 *
 * <p>This authorizer delegates authorization decisions to an Open Policy Agent (OPA) server using a
 * configurable REST API endpoint and policy path. The input to OPA is constructed from the
 * principal, entities, operation, and resource context.
 *
 * <p><strong>Beta Feature:</strong> This implementation is currently in Beta and is not a stable
 * release. It may undergo breaking changes in future versions. Use with caution in production
 * environments.
 */
public class OpaPolarisAuthorizer implements PolarisAuthorizer {
  private final String opaServerUrl;
  private final String opaPolicyPath;
  private final BearerTokenProvider tokenProvider;
  private final CloseableHttpClient httpClient;
  private final ObjectMapper objectMapper;

  /** Private constructor for factory method and advanced wiring. */
  private OpaPolarisAuthorizer(
      String opaServerUrl,
      String opaPolicyPath,
      BearerTokenProvider tokenProvider,
      CloseableHttpClient httpClient,
      ObjectMapper objectMapper) {
    this.opaServerUrl = opaServerUrl;
    this.opaPolicyPath = opaPolicyPath;
    this.tokenProvider = tokenProvider;
    this.httpClient = httpClient;
    this.objectMapper = objectMapper;
  }

  /**
   * Static factory that accepts a BearerTokenProvider for advanced token management.
   *
   * @param opaServerUrl OPA server URL
   * @param opaPolicyPath OPA policy path
   * @param tokenProvider Token provider for authentication (optional)
   * @param client Apache HttpClient (required, injected by CDI). SSL configuration should be
   *     handled by the CDI producer.
   * @return OpaPolarisAuthorizer instance
   */
  public static OpaPolarisAuthorizer create(
      String opaServerUrl,
      String opaPolicyPath,
      BearerTokenProvider tokenProvider,
      @Nonnull CloseableHttpClient client) {

    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(opaServerUrl), "opaServerUrl cannot be null or empty");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(opaPolicyPath), "opaPolicyPath cannot be null or empty");

    try {
      ObjectMapper objectMapperWithDefaults = new ObjectMapper();
      return new OpaPolarisAuthorizer(
          opaServerUrl, opaPolicyPath, tokenProvider, client, objectMapperWithDefaults);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create OpaPolarisAuthorizer", e);
    }
  }

  /**
   * Authorizes a single target and secondary entity for the given principal and operation.
   *
   * <p>Delegates to the multi-target version for consistency.
   *
   * @param polarisPrincipal the principal requesting authorization
   * @param activatedEntities the set of activated entities (roles, etc.)
   * @param authzOp the operation to authorize
   * @param target the main target entity
   * @param secondary the secondary entity (if any)
   * @throws ForbiddenException if authorization is denied by OPA
   */
  @Override
  public void authorizeOrThrow(
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull Set<PolarisBaseEntity> activatedEntities,
      @Nonnull PolarisAuthorizableOperation authzOp,
      @Nullable PolarisResolvedPathWrapper target,
      @Nullable PolarisResolvedPathWrapper secondary) {
    authorizeOrThrow(
        polarisPrincipal,
        activatedEntities,
        authzOp,
        target == null ? null : List.of(target),
        secondary == null ? null : List.of(secondary));
  }

  /**
   * Authorizes one or more target and secondary entities for the given principal and operation.
   *
   * <p>Sends the authorization context to OPA and throws if not allowed.
   *
   * @param polarisPrincipal the principal requesting authorization
   * @param activatedEntities the set of activated entities (roles, etc.)
   * @param authzOp the operation to authorize
   * @param targets the list of main target entities
   * @param secondaries the list of secondary entities (if any)
   * @throws ForbiddenException if authorization is denied by OPA
   */
  @Override
  public void authorizeOrThrow(
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull Set<PolarisBaseEntity> activatedEntities,
      @Nonnull PolarisAuthorizableOperation authzOp,
      @Nullable List<PolarisResolvedPathWrapper> targets,
      @Nullable List<PolarisResolvedPathWrapper> secondaries) {
    boolean allowed = queryOpa(polarisPrincipal, activatedEntities, authzOp, targets, secondaries);
    if (!allowed) {
      throw new ForbiddenException("OPA denied authorization");
    }
  }

  /**
   * Sends an authorization query to the OPA server and parses the response.
   *
   * <p>Builds the OPA input JSON, sends it via HTTP POST, and checks the 'allow' field in the
   * response. The request format follows the OPA REST API specification for data queries.
   *
   * @param principal the principal requesting authorization
   * @param entities the set of activated entities
   * @param op the operation to authorize
   * @param targets the list of main target entities
   * @param secondaries the list of secondary entities (if any)
   * @return true if OPA allows the operation, false otherwise
   * @throws RuntimeException if the OPA query fails
   * @see <a href="https://www.openpolicyagent.org/docs/rest-api">OPA REST API Documentation</a>
   */
  private boolean queryOpa(
      PolarisPrincipal principal,
      Set<PolarisBaseEntity> entities,
      PolarisAuthorizableOperation op,
      List<PolarisResolvedPathWrapper> targets,
      List<PolarisResolvedPathWrapper> secondaries) {
    try {
      String inputJson = buildOpaInputJson(principal, entities, op, targets, secondaries);

      // Create HTTP POST request using Apache HttpComponents
      HttpPost httpPost = new HttpPost(opaServerUrl + opaPolicyPath);
      httpPost.setHeader("Content-Type", "application/json");

      // Add bearer token authentication if provided
      if (tokenProvider != null) {
        String token = tokenProvider.getToken();
        if (token != null && !token.isEmpty()) {
          httpPost.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        }
      }

      httpPost.setEntity(new StringEntity(inputJson, ContentType.APPLICATION_JSON));

      // Execute request
      try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
        int statusCode = response.getCode();
        if (statusCode != 200) {
          return false;
        }

        // Read and parse response
        String responseBody;
        try {
          responseBody = EntityUtils.toString(response.getEntity());
        } catch (ParseException e) {
          throw new RuntimeException("Failed to parse OPA response", e);
        }
        ObjectNode respNode = (ObjectNode) objectMapper.readTree(responseBody);
        return respNode.path("result").path("allow").asBoolean(false);
      }
    } catch (IOException e) {
      throw new RuntimeException("OPA query failed", e);
    }
  }

  /**
   * Builds the OPA input JSON for the authorization query.
   *
   * <p>Assembles the actor, action, resource, and context sections into the expected OPA input
   * format.
   *
   * <p><strong>Note:</strong> OpaPolarisAuthorizer bypasses Polaris's built-in role-based
   * authorization system. This includes both principal roles and catalog roles that would normally
   * be processed by Polaris. Instead, authorization decisions are delegated entirely to the
   * configured OPA policies, which receive the raw principal information and must implement their
   * own role/permission logic.
   *
   * @param principal the principal requesting authorization
   * @param entities the set of activated entities
   * @param op the operation to authorize
   * @param targets the list of main target entities
   * @param secondaries the list of secondary entities (if any)
   * @return the OPA input JSON string
   * @throws IOException if JSON serialization fails
   */
  private String buildOpaInputJson(
      PolarisPrincipal principal,
      Set<PolarisBaseEntity> entities,
      PolarisAuthorizableOperation op,
      List<PolarisResolvedPathWrapper> targets,
      List<PolarisResolvedPathWrapper> secondaries)
      throws IOException {
    ObjectNode input = objectMapper.createObjectNode();
    input.set("actor", buildActorNode(principal));
    input.put("action", op.name());
    input.set("resource", buildResourceNode(targets, secondaries));
    input.set("context", buildContextNode());
    ObjectNode root = objectMapper.createObjectNode();
    root.set("input", input);
    return objectMapper.writeValueAsString(root);
  }

  /**
   * Builds the actor section of the OPA input JSON.
   *
   * <p>Includes principal name, and roles as a generic field.
   *
   * @param principal the principal requesting authorization
   * @return the actor node for OPA input
   */
  private ObjectNode buildActorNode(PolarisPrincipal principal) {
    ObjectNode actor = objectMapper.createObjectNode();
    actor.put("principal", principal.getName());
    ArrayNode roles = objectMapper.createArrayNode();
    for (String role : principal.getRoles()) roles.add(role);
    actor.set("roles", roles);
    return actor;
  }

  /**
   * Builds the resource section of the OPA input JSON.
   *
   * <p>Includes the main target entity under 'primary' and secondary entities under 'secondaries'.
   *
   * @param targets the list of main target entities
   * @param secondaries the list of secondary entities
   * @return the resource node for OPA input
   */
  private ObjectNode buildResourceNode(
      List<PolarisResolvedPathWrapper> targets, List<PolarisResolvedPathWrapper> secondaries) {
    ObjectNode resource = objectMapper.createObjectNode();
    // Main targets as 'targets' array
    ArrayNode targetsArray = objectMapper.createArrayNode();
    if (targets != null && !targets.isEmpty()) {
      for (PolarisResolvedPathWrapper targetWrapper : targets) {
        targetsArray.add(buildSingleResourceNode(targetWrapper));
      }
    }
    resource.set("targets", targetsArray);
    // Secondaries as array
    ArrayNode secondariesArray = objectMapper.createArrayNode();
    if (secondaries != null && !secondaries.isEmpty()) {
      for (PolarisResolvedPathWrapper secondaryWrapper : secondaries) {
        secondariesArray.add(buildSingleResourceNode(secondaryWrapper));
      }
    }
    resource.set("secondaries", secondariesArray);
    return resource;
  }

  /** Helper to build a resource node for a single PolarisResolvedPathWrapper. */
  private ObjectNode buildSingleResourceNode(PolarisResolvedPathWrapper wrapper) {
    ObjectNode node = objectMapper.createObjectNode();
    if (wrapper == null) return node;
    var resolvedEntity = wrapper.getResolvedLeafEntity();
    if (resolvedEntity != null) {
      var entity = resolvedEntity.getEntity();
      node.put("type", entity.getType().name());
      node.put("name", entity.getName());
      var parentPath = wrapper.getResolvedParentPath();
      if (parentPath != null && !parentPath.isEmpty()) {
        ArrayNode parentsArray = objectMapper.createArrayNode();
        for (var parent : parentPath) {
          ObjectNode parentNode = objectMapper.createObjectNode();
          parentNode.put("type", parent.getEntity().getType().name());
          parentNode.put("name", parent.getEntity().getName());
          parentsArray.add(parentNode);
        }
        node.set("parents", parentsArray);
      }
    }
    return node;
  }

  /**
   * Builds the context section of the OPA input JSON.
   *
   * <p>Includes only timestamp and request ID.
   *
   * @return the context node for OPA input
   */
  private ObjectNode buildContextNode() {
    ObjectNode context = objectMapper.createObjectNode();
    context.put("time", java.time.ZonedDateTime.now().toString());
    context.put("request_id", java.util.UUID.randomUUID().toString());
    return context;
  }
}

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
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.extension.auth.opa.model.ImmutableActor;
import org.apache.polaris.extension.auth.opa.model.ImmutableContext;
import org.apache.polaris.extension.auth.opa.model.ImmutableOpaAuthorizationInput;
import org.apache.polaris.extension.auth.opa.model.ImmutableOpaRequest;
import org.apache.polaris.extension.auth.opa.model.ImmutableResource;
import org.apache.polaris.extension.auth.opa.model.ImmutableResourceEntity;
import org.apache.polaris.extension.auth.opa.model.ResourceEntity;
import org.apache.polaris.extension.auth.opa.token.BearerTokenProvider;

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
class OpaPolarisAuthorizer implements PolarisAuthorizer {
  private final URI policyUri;
  private final BearerTokenProvider tokenProvider;
  private final CloseableHttpClient httpClient;
  private final ObjectMapper objectMapper;

  /**
   * Public constructor that accepts a complete policy URI.
   *
   * @param policyUri The required URI for the OPA endpoint. For example,
   *     https://opa.example.com/v1/polaris/allow
   * @param httpClient Apache HttpClient (required, injected by CDI). SSL configuration should be
   *     handled by the CDI producer.
   * @param objectMapper Jackson ObjectMapper for JSON serialization (required). Shared across
   *     authorizer instances to avoid initialization overhead.
   * @param tokenProvider Token provider for authentication (optional)
   */
  public OpaPolarisAuthorizer(
      @Nonnull URI policyUri,
      @Nonnull CloseableHttpClient httpClient,
      @Nonnull ObjectMapper objectMapper,
      @Nullable BearerTokenProvider tokenProvider) {

    this.policyUri = policyUri;
    this.tokenProvider = tokenProvider;
    this.httpClient = httpClient;
    this.objectMapper = objectMapper;
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
      HttpPost httpPost = new HttpPost(policyUri);
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
   * <p>Uses type-safe model classes to construct the authorization input, ensuring consistency with
   * the JSON schema.
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

    // Build actor from principal
    var actor =
        ImmutableActor.builder()
            .principal(principal.getName())
            .addAllRoles(principal.getRoles())
            .build();

    // Build resource entities for targets
    List<ResourceEntity> targetEntities = new ArrayList<>();
    if (targets != null) {
      for (PolarisResolvedPathWrapper target : targets) {
        ResourceEntity entity = buildResourceEntity(target);
        if (entity != null) {
          targetEntities.add(entity);
        }
      }
    }

    // Build resource entities for secondaries
    List<ResourceEntity> secondaryEntities = new ArrayList<>();
    if (secondaries != null) {
      for (PolarisResolvedPathWrapper secondary : secondaries) {
        ResourceEntity entity = buildResourceEntity(secondary);
        if (entity != null) {
          secondaryEntities.add(entity);
        }
      }
    }

    // Build resource
    var resource =
        ImmutableResource.builder().targets(targetEntities).secondaries(secondaryEntities).build();

    // Build context
    var context = ImmutableContext.builder().requestId(UUID.randomUUID().toString()).build();

    // Build complete authorization input
    var input =
        ImmutableOpaAuthorizationInput.builder()
            .actor(actor)
            .action(op.name())
            .resource(resource)
            .context(context)
            .build();

    // Wrap in OPA request
    var request = ImmutableOpaRequest.builder().input(input).build();

    return objectMapper.writeValueAsString(request);
  }

  /**
   * Builds a resource entity from a resolved path wrapper.
   *
   * @param wrapper the resolved path wrapper
   * @return the resource entity, or null if wrapper is null or has no resolved entity
   */
  @Nullable
  private ResourceEntity buildResourceEntity(@Nullable PolarisResolvedPathWrapper wrapper) {
    if (wrapper == null) {
      return null;
    }

    var resolvedEntity = wrapper.getResolvedLeafEntity();
    if (resolvedEntity == null) {
      return null;
    }

    var entity = resolvedEntity.getEntity();
    var builder =
        ImmutableResourceEntity.builder().type(entity.getType().name()).name(entity.getName());

    // Build parent hierarchy
    var parentPath = wrapper.getResolvedParentPath();
    if (parentPath != null && !parentPath.isEmpty()) {
      List<ResourceEntity> parents = new ArrayList<>();
      for (var parent : parentPath) {
        parents.add(
            ImmutableResourceEntity.builder()
                .type(parent.getEntity().getType().name())
                .name(parent.getEntity().getName())
                .build());
      }
      builder.parents(parents);
    }

    return builder.build();
  }
}

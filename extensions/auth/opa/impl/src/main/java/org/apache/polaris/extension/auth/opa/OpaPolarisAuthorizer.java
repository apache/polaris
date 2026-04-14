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
import com.google.common.annotations.VisibleForTesting;
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
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PathSegment;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisSecurable;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
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
   * @param policyUri The required URI for the OPA endpoint. For example, {@code
   *     https://opa.example.com/v1/polaris/allow}.
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
   * Resolves authorization inputs using {@code resolveAll()} for backward compatibility.
   *
   * <p>This scope is intentionally broad for now and will be narrowed in a future refactoring to
   * resolve only the selections required by OPA authorization.
   */
  @Override
  public void resolveAuthorizationInputs(
      @Nonnull AuthorizationState authzState, @Nonnull AuthorizationRequest request) {
    authzState.getResolutionManifest().resolveAll();
  }

  @Override
  @Nonnull
  public AuthorizationDecision authorize(
      @Nonnull AuthorizationState authzState, @Nonnull AuthorizationRequest request) {
    boolean allowed =
        queryOpa(
            buildOpaAuthorizationInput(
                request.getPrincipal(),
                request.getOperation(),
                toResourceEntitiesFromSecurables(request.getTargets()),
                toResourceEntitiesFromSecurables(request.getSecondaries())));
    return allowed
        ? AuthorizationDecision.allow()
        : AuthorizationDecision.deny(
            "OPA denied authorization for " + request.formatForAuthorizationMessage());
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
    boolean allowed =
        queryOpa(
            buildOpaAuthorizationInput(
                polarisPrincipal,
                authzOp,
                toResourceEntitiesFromResolvedPaths(targets),
                toResourceEntitiesFromResolvedPaths(secondaries)));
    if (!allowed) {
      throw new ForbiddenException(
          "OPA denied authorization for operation=%s principal=%s targets=%s secondaries=%s",
          authzOp, polarisPrincipal.getName(), targets, secondaries);
    }
  }

  /**
   * Sends an authorization query to the OPA server and parses the response.
   *
   * <p>Builds the OPA input JSON, sends it via HTTP POST, and checks the 'allow' field in the
   * response. The request format follows the OPA REST API specification for data queries.
   *
   * @param input OPA authorization input model
   * @return true if OPA allows the operation, false otherwise
   * @throws RuntimeException if the OPA query fails
   * @see <a href="https://www.openpolicyagent.org/docs/rest-api">OPA REST API Documentation</a>
   */
  private boolean queryOpa(ImmutableOpaAuthorizationInput input) {
    try {
      String inputJson = buildOpaInputJson(input);

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
      return httpClientExecute(httpPost, this::queryOpaCheckResponse);
    } catch (HttpException | IOException e) {
      throw new RuntimeException("OPA query failed", e);
    }
  }

  @VisibleForTesting
  <T> T httpClientExecute(
      ClassicHttpRequest request, HttpClientResponseHandler<? extends T> responseHandler)
      throws HttpException, IOException {
    return httpClient.execute(request, responseHandler);
  }

  private boolean queryOpaCheckResponse(ClassicHttpResponse response) throws IOException {
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
   * @param input OPA authorization input model
   * @return the OPA input JSON string
   * @throws IOException if JSON serialization fails
   */
  private String buildOpaInputJson(ImmutableOpaAuthorizationInput input) throws IOException {
    // Wrap in OPA request
    var request = ImmutableOpaRequest.builder().input(input).build();
    return objectMapper.writeValueAsString(request);
  }

  private ImmutableOpaAuthorizationInput buildOpaAuthorizationInput(
      PolarisPrincipal principal,
      PolarisAuthorizableOperation op,
      List<ResourceEntity> targets,
      List<ResourceEntity> secondaries) {
    return ImmutableOpaAuthorizationInput.builder()
        .actor(buildActor(principal))
        .action(op.name())
        .resource(buildResource(targets, secondaries))
        .context(buildContext())
        .build();
  }

  private ImmutableActor buildActor(PolarisPrincipal principal) {
    return ImmutableActor.builder()
        .principal(principal.getName())
        .addAllRoles(principal.getRoles())
        .build();
  }

  private ImmutableContext buildContext() {
    return ImmutableContext.builder().requestId(UUID.randomUUID().toString()).build();
  }

  private ImmutableResource buildResource(
      List<ResourceEntity> targets, List<ResourceEntity> secondaries) {
    // Backward compatibility: keep the existing OPA input shape with separate target and
    // secondary lists. Future work can align this with AuthorizationTargetBinding semantics
    // using binding tuples like [(target, secondary), ...].
    return ImmutableResource.builder().targets(targets).secondaries(secondaries).build();
  }

  private ResourceEntity buildResourceEntity(PolarisSecurable securable) {
    // This is the target shape we want going forward where we derive the OPA payload from
    // PolarisSecurable. This will exclude RBAC-only concepts like ROOT container.
    PathSegment leaf = securable.getLeaf();
    var builder =
        ImmutableResourceEntity.builder().type(leaf.entityType().name()).name(leaf.name());
    List<ResourceEntity> parents = new ArrayList<>();
    for (PathSegment parent : securable.getParents()) {
      parents.add(
          ImmutableResourceEntity.builder()
              .type(parent.entityType().name())
              .name(parent.name())
              .build());
    }
    builder.parents(parents);
    return builder.build();
  }

  private ResourceEntity buildResourceEntity(PolarisResolvedPathWrapper path) {
    // Currently, authorizeOrThrow still evaluate through resolved paths, including
    // root-scoped operations that may surface a resolved ROOT leaf. Preserve that legacy
    // behavior for compatibility until those callers migrate to the intent-based flow.
    ResolvedPolarisEntity resolvedLeaf = path.getResolvedLeafEntity();
    PathSegment leaf =
        new PathSegment(resolvedLeaf.getEntity().getType(), resolvedLeaf.getEntity().getName());
    List<ResourceEntity> parents = new ArrayList<>();
    List<ResolvedPolarisEntity> resolvedParents = path.getResolvedParentPath();
    if (resolvedParents != null) {
      for (ResolvedPolarisEntity resolvedParent : resolvedParents) {
        parents.add(
            ImmutableResourceEntity.builder()
                .type(resolvedParent.getEntity().getType().name())
                .name(resolvedParent.getEntity().getName())
                .build());
      }
    }
    return ImmutableResourceEntity.builder()
        .type(leaf.entityType().name())
        .name(leaf.name())
        .parents(parents)
        .build();
  }

  @Nonnull
  private List<ResourceEntity> toResourceEntitiesFromResolvedPaths(
      @Nullable List<PolarisResolvedPathWrapper> paths) {
    if (paths == null || paths.isEmpty()) {
      return List.of();
    }

    List<ResourceEntity> entities = new ArrayList<>();
    for (PolarisResolvedPathWrapper path : paths) {
      if (path != null && path.getResolvedLeafEntity() != null) {
        entities.add(buildResourceEntity(path));
      }
    }
    return entities;
  }

  @Nonnull
  private List<ResourceEntity> toResourceEntitiesFromSecurables(
      @Nullable List<PolarisSecurable> securables) {
    if (securables == null || securables.isEmpty()) {
      return List.of();
    }

    List<ResourceEntity> entities = new ArrayList<>();
    for (PolarisSecurable securable : securables) {
      entities.add(buildResourceEntity(securable));
    }
    return entities;
  }
}

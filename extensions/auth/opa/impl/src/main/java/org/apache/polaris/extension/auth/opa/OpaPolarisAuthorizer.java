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
import java.util.EnumSet;
import java.util.List;
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
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisSecurable;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.Resolvable;
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
  public void resolveAuthorizationInputs(
      @Nonnull AuthorizationState ctx, @Nonnull AuthorizationRequest request) {
    PolarisResolutionManifest manifest = ctx.getResolutionManifest();
    if (manifest.hasResolution()) {
      return;
    }
    // Resolve requested entities without RBAC role resolution.
    manifest.resolveSelections(
        EnumSet.of(
            Resolvable.REFERENCE_CATALOG,
            Resolvable.REQUESTED_PATHS,
            Resolvable.REQUESTED_TOP_LEVEL_ENTITIES));
  }

  @Override
  public void authorize(@Nonnull AuthorizationState ctx, @Nonnull AuthorizationRequest request) {
    if (request.isInternalPrincipalScope()
        || request.isInternalPrincipalRoleScope()
        || request.isInternalCatalogRoleScope()) {
      throw new ForbiddenException("OPA denied admin operation");
    }
    boolean allowed =
        queryOpaIntent(
            request.getPrincipal(),
            request.getOperation(),
            request.getTargets(),
            request.getSecondaries());
    if (!allowed) {
      throw new ForbiddenException(
          "Principal '%s' is not authorized for op %s",
          request.getPrincipal().getName(), request.getOperation());
    }
  }

  private boolean queryOpaIntent(
      PolarisPrincipal principal,
      PolarisAuthorizableOperation op,
      List<PolarisSecurable> targets,
      List<PolarisSecurable> secondaries) {
    try {
      String inputJson = buildOpaInputJson(principal, op, targets, secondaries);

      HttpPost httpPost = new HttpPost(policyUri);
      httpPost.setHeader("Content-Type", "application/json");

      if (tokenProvider != null) {
        String token = tokenProvider.getToken();
        if (token != null && !token.isEmpty()) {
          httpPost.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        }
      }

      httpPost.setEntity(new StringEntity(inputJson, ContentType.APPLICATION_JSON));
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

  private String buildOpaInputJson(
      PolarisPrincipal principal,
      PolarisAuthorizableOperation op,
      List<PolarisSecurable> targets,
      List<PolarisSecurable> secondaries)
      throws IOException {

    var actor =
        ImmutableActor.builder()
            .principal(principal.getName())
            .addAllRoles(principal.getRoles())
            .build();

    List<ResourceEntity> targetEntities = new ArrayList<>();
    if (targets != null) {
      for (PolarisSecurable target : targets) {
        ResourceEntity entity = buildResourceEntity(target);
        if (entity != null) {
          targetEntities.add(entity);
        }
      }
    }

    List<ResourceEntity> secondaryEntities = new ArrayList<>();
    if (secondaries != null) {
      for (PolarisSecurable secondary : secondaries) {
        ResourceEntity entity = buildResourceEntity(secondary);
        if (entity != null) {
          secondaryEntities.add(entity);
        }
      }
    }

    var resource =
        ImmutableResource.builder().targets(targetEntities).secondaries(secondaryEntities).build();
    var context = ImmutableContext.builder().requestId(UUID.randomUUID().toString()).build();
    var input =
        ImmutableOpaAuthorizationInput.builder()
            .actor(actor)
            .action(op.name())
            .resource(resource)
            .context(context)
            .build();
    var request = ImmutableOpaRequest.builder().input(input).build();

    return objectMapper.writeValueAsString(request);
  }

  private ResourceEntity buildResourceEntity(@Nullable PolarisSecurable securable) {
    if (securable == null) {
      return null;
    }

    var builder =
        ImmutableResourceEntity.builder()
            .type(securable.getEntityType().name())
            .name(
                securable.getNameParts().isEmpty()
                    ? ""
                    : securable.getNameParts().get(securable.getNameParts().size() - 1));

    List<String> parts = securable.getNameParts();
    if (parts.size() > 1) {
      List<ResourceEntity> parents = new ArrayList<>();
      PolarisEntityType parentType =
          switch (securable.getEntityType()) {
            case TABLE_LIKE, POLICY, NAMESPACE -> PolarisEntityType.NAMESPACE;
            default -> null;
          };
      if (parentType != null) {
        for (int i = 0; i < parts.size() - 1; i++) {
          parents.add(
              ImmutableResourceEntity.builder().type(parentType.name()).name(parts.get(i)).build());
        }
        builder.parents(parents);
      }
    }

    return builder.build();
  }
}

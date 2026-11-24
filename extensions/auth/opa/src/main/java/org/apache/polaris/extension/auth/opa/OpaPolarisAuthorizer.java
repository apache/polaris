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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.apache.polaris.core.auth.AuthorizationBatchRequest;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationIntent;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.NonRBACResolutionSemantics;
import org.apache.polaris.core.auth.PathSegment;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributePrefixes;
import org.apache.polaris.core.auth.PolarisSecurable;
import org.apache.polaris.core.auth.PolicyAttachmentAuthorizationIntent;
import org.apache.polaris.core.auth.PrivilegeGrantAuthorizationIntent;
import org.apache.polaris.core.auth.RenameAuthorizationIntent;
import org.apache.polaris.core.auth.RoleAssignmentAuthorizationIntent;
import org.apache.polaris.core.auth.RootPrivilegeGrantAuthorizationIntent;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.core.auth.TargetlessAuthorizationIntent;
import org.apache.polaris.core.persistence.resolver.Resolvable;
import org.apache.polaris.extension.auth.opa.model.ImmutableActor;
import org.apache.polaris.extension.auth.opa.model.ImmutableContext;
import org.apache.polaris.extension.auth.opa.model.ImmutableOpaAuthorizationInput;
import org.apache.polaris.extension.auth.opa.model.ImmutableOpaBatchAuthorizationInput;
import org.apache.polaris.extension.auth.opa.model.ImmutableOpaBatchQuery;
import org.apache.polaris.extension.auth.opa.model.ImmutableOpaBatchRequest;
import org.apache.polaris.extension.auth.opa.model.ImmutableOpaRequest;
import org.apache.polaris.extension.auth.opa.model.ImmutableResource;
import org.apache.polaris.extension.auth.opa.model.ImmutableResourceEntity;
import org.apache.polaris.extension.auth.opa.model.ResourceEntity;
import org.apache.polaris.extension.auth.opa.token.BearerTokenProvider;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOGGER = LoggerFactory.getLogger(OpaPolarisAuthorizer.class);

  private final URI policyUri;
  private final @Nullable URI batchPolicyUri;
  private final int maxBatchSize;
  private final BearerTokenProvider tokenProvider;
  private final CloseableHttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final String requestId;
  private final String realm;
  private final Map<String, String> contextProperties;

  /**
   * Public constructor that accepts a complete policy URI and the current realm identifier.
   *
   * @param policyUri The required URI for the OPA endpoint. For example, {@code
   *     https://opa.example.com/v1/polaris/allow}.
   * @param batchPolicyUri The optional URI of the OPA batch rule. When {@code null}, batch requests
   *     are answered with one call per question to {@code policyUri}.
   * @param maxBatchSize The maximum number of questions sent to OPA in one batch call.
   * @param httpClient Apache HttpClient (required, injected by CDI). SSL configuration should be
   *     handled by the CDI producer.
   * @param objectMapper Jackson ObjectMapper for JSON serialization (required). Shared across
   *     authorizer instances to avoid initialization overhead.
   * @param tokenProvider Token provider for authentication (optional)
   * @param requestId The server-generated request ID (optional), used to correlate OPA queries with
   *     the originating HTTP request. Resolved once by the caller since this authorizer is
   *     constructed fresh per request.
   * @param realm The realm identifier (from RealmContext) for isolation in OPA policies.
   * @param contextProperties Operator-defined static properties emitted verbatim under {@code
   *     context.properties} in the OPA input. Empty when none are configured.
   */
  public OpaPolarisAuthorizer(
      @NonNull URI policyUri,
      @Nullable URI batchPolicyUri,
      int maxBatchSize,
      @NonNull CloseableHttpClient httpClient,
      @NonNull ObjectMapper objectMapper,
      @Nullable BearerTokenProvider tokenProvider,
      @Nullable String requestId,
      @NonNull String realm,
      @NonNull Map<String, String> contextProperties) {

    this.policyUri = policyUri;
    this.batchPolicyUri = batchPolicyUri;
    this.maxBatchSize = maxBatchSize;
    this.tokenProvider = tokenProvider;
    this.httpClient = httpClient;
    this.objectMapper = objectMapper;
    this.requestId = requestId;
    this.realm = realm;
    this.contextProperties = contextProperties;
  }

  /** Creates an authorizer without batch support: batch requests use one call per question. */
  public OpaPolarisAuthorizer(
      @NonNull URI policyUri,
      @NonNull CloseableHttpClient httpClient,
      @NonNull ObjectMapper objectMapper,
      @Nullable BearerTokenProvider tokenProvider,
      @Nullable String requestId,
      @NonNull String realm,
      @NonNull Map<String, String> contextProperties) {
    this(
        policyUri,
        null,
        Integer.parseInt(OpaAuthorizationConfig.DEFAULT_MAX_BATCH_SIZE),
        httpClient,
        objectMapper,
        tokenProvider,
        requestId,
        realm,
        contextProperties);
  }

  /**
   * {@link AuthorizationState#resolve() Resolves} authorization inputs using the narrowest set of
   * {@link Resolvable}s that is required for authorizing the given {code request}.
   *
   * @see AuthorizationState#resolve()
   */
  @Override
  public void resolveAuthorizationInputs(
      @NonNull AuthorizationState authzState, @NonNull AuthorizationRequest request) {
    NonRBACResolutionSemantics.applySelectors(authzState, request);
    authzState.resolve();
  }

  @Override
  @NonNull
  public AuthorizationDecision authorize(
      @NonNull AuthorizationState authzState, @NonNull AuthorizationRequest request) {
    for (AuthorizationIntent intent : request.intents()) {
      PolarisAuthorizableOperation operation = intent.operation();
      IntentResources resources = toResources(intent);
      List<ResourceEntity> targets = resources.targets();
      List<ResourceEntity> secondaries = resources.secondaries();
      boolean allowed =
          queryOpa(
              buildOpaAuthorizationInput(request.principal(), operation, targets, secondaries));
      if (!allowed) {
        LOGGER.debug(
            "OPA denied authorization for principal={} operation={} targets={} secondaries={} realm={} requestId={}",
            request.principal().getName(),
            operation,
            targets,
            secondaries,
            realm,
            requestId);
        return AuthorizationDecision.deny("OPA denied authorization");
      }
    }
    return AuthorizationDecision.allow();
  }

  /**
   * Authorizes every intent independently. With a batch policy URI configured, the intents are sent
   * to OPA in chunks of at most {@code maxBatchSize}, one HTTP call per chunk. Without one, this
   * falls back to one call per intent.
   *
   * <p>Any problem with a batch call fails the whole batch: a missing, extra or non-boolean answer
   * is never treated as a deny, so a misconfigured policy cannot silently hide entities.
   */
  @Override
  public @NonNull List<AuthorizationDecision> authorize(
      @NonNull AuthorizationState authzState, @NonNull AuthorizationBatchRequest batch) {
    if (batchPolicyUri == null || batch.intents().isEmpty()) {
      return PolarisAuthorizer.super.authorize(authzState, batch);
    }
    ImmutableActor actor = buildActor(batch.principal());
    ImmutableContext context = buildContext();
    List<AuthorizationDecision> decisions = new ArrayList<>(batch.intents().size());
    int chunkIndex = 0;
    for (List<AuthorizationIntent> chunk : Lists.partition(batch.intents(), maxBatchSize)) {
      decisions.addAll(queryOpaBatch(batch.principal(), actor, context, chunk, chunkIndex++));
    }
    return List.copyOf(decisions);
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
      return postJson(policyUri, buildOpaInputJson(input), this::queryOpaCheckResponse);
    } catch (HttpException | IOException e) {
      throw new RuntimeException("OPA query failed", e);
    }
  }

  /**
   * POSTs a JSON body to OPA, adding the bearer token when one is configured, and handles the
   * response with {@code responseHandler}.
   */
  private <T> T postJson(
      URI uri, String json, HttpClientResponseHandler<? extends T> responseHandler)
      throws HttpException, IOException {
    // Create HTTP POST request using Apache HttpComponents
    HttpPost httpPost = new HttpPost(uri);
    httpPost.setHeader("Content-Type", "application/json");

    // Add bearer token authentication if provided
    if (tokenProvider != null) {
      String token = tokenProvider.getToken();
      if (token != null && !token.isEmpty()) {
        httpPost.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token);
      }
    }

    httpPost.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));

    // Execute request
    return httpClientExecute(httpPost, responseHandler);
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
      LOGGER.warn("OPA returned unexpected HTTP status {}, treating as deny", statusCode);
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
   * Asks OPA's batch rule about one chunk of intents. Questions are identified by their index in
   * the chunk, and the response maps each of those IDs to a boolean.
   */
  private List<AuthorizationDecision> queryOpaBatch(
      PolarisPrincipal principal,
      ImmutableActor actor,
      ImmutableContext context,
      List<AuthorizationIntent> chunk,
      int chunkIndex) {
    var input = ImmutableOpaBatchAuthorizationInput.builder().actor(actor).context(context);
    for (int i = 0; i < chunk.size(); i++) {
      input.addRequests(buildOpaBatchQuery(String.valueOf(i), chunk.get(i)));
    }

    try {
      String json =
          objectMapper.writeValueAsString(
              ImmutableOpaBatchRequest.builder().input(input.build()).build());
      JsonNode result = postJson(batchPolicyUri, json, this::queryOpaBatchCheckResponse);
      List<AuthorizationDecision> decisions = toDecisions(result, chunk.size());
      LOGGER.debug(
          "OPA batch chunk={} size={} denied={} realm={} requestId={}",
          chunkIndex,
          chunk.size(),
          decisions.stream().filter(d -> !d.isAllowed()).count(),
          realm,
          requestId);
      return decisions;
    } catch (HttpException | IOException | RuntimeException e) {
      LOGGER.warn(
          "OPA batch query failed for principal={} chunk={} size={} realm={} requestId={}: {}",
          principal.getName(),
          chunkIndex,
          chunk.size(),
          realm,
          requestId,
          e.getMessage());
      throw new RuntimeException("OPA batch query failed: " + e.getMessage(), e);
    }
  }

  private JsonNode queryOpaBatchCheckResponse(ClassicHttpResponse response) throws IOException {
    int statusCode = response.getCode();
    if (statusCode != 200) {
      throw new IOException("unexpected HTTP status " + statusCode);
    }
    try {
      return objectMapper.readTree(EntityUtils.toString(response.getEntity()));
    } catch (ParseException e) {
      throw new IOException("failed to read the response", e);
    }
  }

  /**
   * Maps the {@code result} of a batch response to one decision per question, in question order.
   * Every ID {@code 0..size-1} must be present with a JSON boolean, and no other IDs may appear.
   */
  private static List<AuthorizationDecision> toDecisions(JsonNode response, int size) {
    JsonNode result = response.path("result");
    if (!result.isObject()) {
      throw new IllegalStateException(
          "the response has no 'result' object; is the batch rule defined?");
    }
    List<AuthorizationDecision> decisions = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      JsonNode answer = result.get(String.valueOf(i));
      if (answer == null) {
        throw new IllegalStateException("no answer for question " + i);
      }
      if (!answer.isBoolean()) {
        throw new IllegalStateException("the answer for question " + i + " is not a boolean");
      }
      decisions.add(
          answer.booleanValue()
              ? AuthorizationDecision.allow()
              : AuthorizationDecision.deny("OPA denied authorization"));
    }
    if (result.size() != size) {
      throw new IllegalStateException(
          "expected " + size + " answers but the response has " + result.size());
    }
    return decisions;
  }

  /** The resource entities an intent acts on: its primary targets and its secondary entities. */
  private record IntentResources(List<ResourceEntity> targets, List<ResourceEntity> secondaries) {}

  private IntentResources toResources(AuthorizationIntent intent) {
    return switch (intent) {
      case TargetlessAuthorizationIntent ignored -> new IntentResources(List.of(), List.of());
      case SingleTargetAuthorizationIntent singleTargetIntent ->
          new IntentResources(
              toResourceEntitiesFromSecurable(singleTargetIntent.target()), List.of());
      case RenameAuthorizationIntent renameIntent ->
          new IntentResources(
              toResourceEntitiesFromSecurable(renameIntent.from()),
              toResourceEntitiesFromSecurable(renameIntent.to()));
      case PolicyAttachmentAuthorizationIntent policyAttachmentIntent ->
          new IntentResources(
              toResourceEntitiesFromSecurable(policyAttachmentIntent.policy()),
              toResourceEntitiesFromSecurable(policyAttachmentIntent.attachedTo()));
      case RoleAssignmentAuthorizationIntent roleAssignmentIntent ->
          new IntentResources(
              toResourceEntitiesFromSecurable(roleAssignmentIntent.role()),
              toResourceEntitiesFromSecurable(roleAssignmentIntent.assignee()));
      case PrivilegeGrantAuthorizationIntent privilegeGrantIntent ->
          new IntentResources(
              toResourceEntitiesFromSecurable(privilegeGrantIntent.grantTarget()),
              toResourceEntitiesFromSecurable(privilegeGrantIntent.grantee()));
      case RootPrivilegeGrantAuthorizationIntent rootPrivilegeGrantIntent ->
          new IntentResources(
              List.of(), toResourceEntitiesFromSecurable(rootPrivilegeGrantIntent.grantee()));
    };
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

  private ImmutableOpaBatchQuery buildOpaBatchQuery(String id, AuthorizationIntent intent) {
    IntentResources resources = toResources(intent);
    return ImmutableOpaBatchQuery.builder()
        .id(id)
        .action(intent.operation().name())
        .resource(buildResource(resources.targets(), resources.secondaries()))
        .build();
  }

  private ImmutableActor buildActor(PolarisPrincipal principal) {
    return ImmutableActor.builder()
        .principal(principal.getName())
        .addAllRoles(principal.getRoles())
        .putAllAttributes(PolarisPrincipalAttributePrefixes.derivedStringAttributes(principal))
        .build();
  }

  private ImmutableContext buildContext() {
    return ImmutableContext.builder()
        .requestId(requestId != null ? requestId : UUID.randomUUID().toString())
        .realm(realm)
        .putAllProperties(contextProperties)
        .build();
  }

  private ImmutableResource buildResource(
      List<ResourceEntity> targets, List<ResourceEntity> secondaries) {
    // Keep the existing OPA input shape by always emitting target and secondary lists, using
    // empty lists when an intent does not carry that slot. Future work can revisit the payload
    // shape if OPA starts consuming intent subtype distinctions directly.
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

  private List<ResourceEntity> toResourceEntitiesFromSecurable(
      @Nullable PolarisSecurable securable) {
    return securable == null ? List.of() : List.of(buildResourceEntity(securable));
  }
}

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
package org.apache.polaris.service.idempotency;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.core.Vertx;
import jakarta.inject.Inject;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.IdempotencyRecord;
import org.apache.polaris.core.persistence.IdempotencyStore;
import org.apache.polaris.core.persistence.IdempotencyStore.HeartbeatResult;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.resolver.Resolver;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalogAdapter;
import org.apache.polaris.service.context.RealmContextFilter;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.server.ServerRequestFilter;
import org.jboss.resteasy.reactive.server.ServerResponseFilter;
import org.jboss.resteasy.reactive.server.core.CurrentRequestManager;
import org.jboss.resteasy.reactive.server.core.ResteasyReactiveRequestContext;
import org.jboss.resteasy.reactive.server.spi.ResteasyReactiveResourceInfo;

/**
 * HTTP idempotency integration at the request/response layer.
 *
 * <p>For in-scope requests (see {@link IdempotencyConfiguration#scopes()}), this filter reserves an
 * idempotency key before executing the request, and replays the previously finalized response when
 * a duplicate key is received. For owned requests, it finalizes the response summary/headers for
 * future replay.
 *
 * <p>Replayed responses use the full persisted response entity; responses are not truncated.
 */
public class IdempotencyFilter {
  private static final Logger LOG = Logger.getLogger(IdempotencyFilter.class);

  private static final Set<Integer> NEVER_FINALIZE_STATUSES = Set.of(401, 403, 408, 429);

  private static final long WARN_THROTTLE_MS = 60_000L;
  private static final AtomicLong LAST_WARN_AT_MS = new AtomicLong(0L);

  private static final int FINALIZE_MAX_RETRIES = 3;
  private static final long FINALIZE_RETRY_BASE_MS = 200;

  private static final String PROP_IDEMPOTENCY_KEY = "idempotency.key";
  private static final String PROP_IDEMPOTENCY_OPERATION = "idempotency.operation";
  private static final String PROP_IDEMPOTENCY_RESOURCE = "idempotency.resource";
  private static final String PROP_IDEMPOTENCY_OWNED = "idempotency.owned";
  private static final String PROP_IDEMPOTENCY_HEARTBEAT_TIMER_ID = "idempotency.heartbeat.timerId";

  @Inject IdempotencyConfiguration configuration;
  @Inject IdempotencyStore store;
  @Inject Clock clock;
  @Inject ObjectMapper objectMapper;
  @Inject Vertx vertx;
  @Inject ResolverFactory resolverFactory;
  @Inject CatalogPrefixParser prefixParser;

  private volatile String resolvedExecutorId;

  @ServerRequestFilter(priority = Priorities.AUTHORIZATION + 10)
  public Uni<Response> reserveOrReplay(ContainerRequestContext rc) {
    if (!configuration.enabled()) {
      return Uni.createFrom().nullItem();
    }

    SecurityContext securityContext = rc.getSecurityContext();

    String rawKey = rc.getHeaderString(configuration.keyHeader());
    if (rawKey == null || rawKey.isBlank()) {
      return Uni.createFrom().nullItem();
    }
    String key = normalizeIdempotencyKey(rawKey);
    if (key == null) {
      return Uni.createFrom()
          .item(
              error(
                  400,
                  "idempotency_key_invalid",
                  "Idempotency-Key must be a UUIDv7 string (RFC 9562)"));
    }

    boolean scopedMode = !configuration.scopes().isEmpty();
    if (!scopedMode) {
      // Without an explicit scope allowlist, only apply idempotency to Iceberg REST catalog
      // mutating endpoints under /v1/{prefix}/...
      //
      // This check is best-effort because request-to-resource matching metadata may be unavailable
      // in some cases; if we can't confidently identify an Iceberg mutation endpoint we fail closed
      // (treat idempotency as a no-op) to avoid false positives.
      if (!isMutatingMethod(rc.getMethod()) || !isIcebergMutationEndpoint(rc)) {
        return Uni.createFrom().nullItem();
      }
    }

    RealmContext realmContext = (RealmContext) rc.getProperty(RealmContextFilter.REALM_CONTEXT_KEY);
    if (realmContext == null) {
      // RealmContextFilter should run before this; treat missing realm as a server error.
      return Uni.createFrom().item(error(500, "MissingRealmContext", "Missing realm context"));
    }

    // If scopes are configured, apply idempotency only to matching endpoints and use the configured
    // operationType for stable binding.
    IdempotencyConfiguration.Scope scope = matchScope(rc);
    if (scope == null && scopedMode) {
      return Uni.createFrom().nullItem();
    }

    PolarisPrincipal principal =
        securityContext != null && securityContext.getUserPrincipal() instanceof PolarisPrincipal p
            ? p
            : null;
    final String requestPath = rc.getUriInfo().getPath();
    Uni<Boolean> internalCatalogCheck =
        principal == null
            ? Uni.createFrom().item(true)
            : Uni.createFrom()
                .item(() -> isPolarisManagedInternalIcebergCatalogRequest(requestPath, principal))
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                .onFailure()
                .recoverWithItem(false);

    String realmId = realmContext.getRealmIdentifier();
    String operationType = scope == null ? normalizeOperationType(rc) : scope.operationType();
    // Use path only to avoid accidental mismatches from query ordering/irrelevant parameters.
    String resourceId = rc.getUriInfo().getPath();

    Instant now = clock.instant();
    Instant expiresAt = now.plus(configuration.ttl()).plus(configuration.ttlGrace());

    return internalCatalogCheck
        .onItem()
        .transformToUni(
            internal -> {
              if (!internal) {
                // For federated/external catalogs, Polaris may not enforce idempotency end-to-end,
                // so treat Idempotency-Key as a no-op (do not reserve/finalize/replay).
                return Uni.createFrom().nullItem();
              }
              return Uni.createFrom()
                  .item(
                      () ->
                          store.reserve(
                              realmId,
                              key,
                              operationType,
                              resourceId,
                              expiresAt,
                              executorId(),
                              now))
                  .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                  .onItem()
                  .transformToUni(
                      r -> {
                        if (r.type() == IdempotencyStore.ReserveResultType.OWNED) {
                          rc.setProperty(PROP_IDEMPOTENCY_KEY, key);
                          rc.setProperty(PROP_IDEMPOTENCY_OPERATION, operationType);
                          rc.setProperty(PROP_IDEMPOTENCY_RESOURCE, resourceId);
                          rc.setProperty(PROP_IDEMPOTENCY_OWNED, Boolean.TRUE);
                          maybeStartHeartbeat(rc, realmId, key);
                          return Uni.createFrom().nullItem();
                        }

                        Optional<IdempotencyRecord> existingOpt = r.existing();
                        if (existingOpt.isEmpty()) {
                          // Should not happen: DUPLICATE should always return an existing record.
                          return Uni.createFrom()
                              .item(
                                  error(
                                      500,
                                      "IdempotencyInvariantViolation",
                                      "Missing existing record"));
                        }

                        IdempotencyRecord existing = existingOpt.get();
                        if (!operationType.equals(existing.operationType())
                            || !resourceId.equals(existing.normalizedResourceId())) {
                          return Uni.createFrom()
                              .item(
                                  error(
                                      422,
                                      "idempotency_key_conflict",
                                      "Idempotency key was already used for a different operation/resource"));
                        }

                        if (!existing.isFinalized()) {
                          if (configuration.heartbeatEnabled()) {
                            // If the owner appears stale (no recent heartbeat), don't wait
                            // indefinitely. ttlGrace pads the check to tolerate clock skew
                            // between nodes. Full reconciliation/takeover is a follow-up.
                            Instant checkNow = clock.instant();
                            Instant lastSignal = existing.heartbeatAt();
                            if (lastSignal == null) {
                              lastSignal = existing.updatedAt();
                              if (lastSignal == null) {
                                lastSignal = existing.createdAt();
                              }
                            }
                            Duration staleThreshold =
                                configuration.leaseTtl().plus(configuration.ttlGrace());
                            if (lastSignal == null
                                || Duration.between(lastSignal, checkNow).compareTo(staleThreshold)
                                    > 0) {
                              return Uni.createFrom()
                                  .item(
                                      error(
                                          503,
                                          "idempotency_in_progress",
                                          "Operation with this idempotency key may be stale; retry later"));
                            }
                          }
                          Instant deadline = clock.instant().plus(configuration.inProgressWait());
                          return waitForFinalized(realmId, key, deadline)
                              .onItem()
                              .transform(this::replayResponse)
                              .onFailure(TimeoutException.class)
                              .recoverWithItem(
                                  error(
                                      503,
                                      "idempotency_in_progress",
                                      "Operation with this idempotency key is still in progress; retry later"))
                              .onFailure()
                              .recoverWithItem(
                                  error(
                                      503,
                                      "idempotency_store_unavailable",
                                      "Idempotency store unavailable; retry later"));
                        }

                        return Uni.createFrom().item(replayResponse(existing));
                      })
                  .onFailure()
                  .recoverWithItem(
                      error(
                          503,
                          "idempotency_store_unavailable",
                          "Idempotency store unavailable; retry later"));
            });
  }

  @ServerResponseFilter
  public void finalizeOwned(ContainerRequestContext request, ContainerResponseContext response) {
    if (!configuration.enabled()) {
      return;
    }
    if (!Boolean.TRUE.equals(request.getProperty(PROP_IDEMPOTENCY_OWNED))) {
      return;
    }

    stopHeartbeat(request);

    RealmContext realmContext =
        (RealmContext) request.getProperty(RealmContextFilter.REALM_CONTEXT_KEY);
    if (realmContext == null) {
      return;
    }

    String realmId = realmContext.getRealmIdentifier();
    String key = (String) request.getProperty(PROP_IDEMPOTENCY_KEY);
    if (key == null) {
      return;
    }

    int status = response.getStatus();
    if (!shouldFinalize(status)) {
      // Do not finalize/replay for 401/403/408/429 or any 5xx. If we reserved this key for such a
      // request, release the in-progress record so the key does not linger and block retries.
      if (shouldCancelInProgressReservation(status)) {
        Infrastructure.getDefaultWorkerPool()
            .execute(
                () -> {
                  try {
                    store.cancelInProgressReservation(realmId, key, executorId());
                  } catch (RuntimeException e) {
                    warnThrottled(
                        e,
                        "Failed to cancel in-progress idempotency reservation; replay may be unavailable");
                  }
                });
      }
      return;
    }
    final String body =
        boundedResponseSummary(response, objectMapper, configuration.responseSummaryMaxBytes());
    final Map<String, String> headers =
        headerSnapshot(response, configuration.responseHeaderAllowlist());
    Instant now = clock.instant();

    Infrastructure.getDefaultWorkerPool()
        .execute(
            () -> {
              for (int attempt = 1; attempt <= FINALIZE_MAX_RETRIES; attempt++) {
                try {
                  store.finalizeRecord(
                      realmId, key, executorId(), status, null, body, headers, now);
                  return;
                } catch (RuntimeException e) {
                  if (attempt < FINALIZE_MAX_RETRIES) {
                    LOG.debugf(
                        e,
                        "Finalize attempt %d/%d failed for key %s; retrying",
                        attempt,
                        FINALIZE_MAX_RETRIES,
                        key);
                    try {
                      Thread.sleep(FINALIZE_RETRY_BASE_MS * attempt);
                    } catch (InterruptedException ie) {
                      Thread.currentThread().interrupt();
                      LOG.warn(
                          "Interrupted while retrying finalize; record may remain in-progress", e);
                      return;
                    }
                  } else {
                    LOG.warnf(
                        e,
                        "Failed to finalize idempotency record after %d attempts for key %s; "
                            + "record will remain in-progress until TTL expiry",
                        FINALIZE_MAX_RETRIES,
                        key);
                  }
                }
              }
            });
  }

  private void maybeStartHeartbeat(ContainerRequestContext rc, String realmId, String key) {
    if (!configuration.heartbeatEnabled()) {
      return;
    }
    long intervalMs = configuration.heartbeatInterval().toMillis();
    long timerId =
        vertx.setPeriodic(
            intervalMs,
            tid -> {
              Infrastructure.getDefaultWorkerPool()
                  .execute(
                      () -> {
                        try {
                          HeartbeatResult hr =
                              store.updateHeartbeat(realmId, key, executorId(), clock.instant());
                          if (hr != HeartbeatResult.UPDATED) {
                            vertx.cancelTimer(tid);
                            LOG.debugf("Stopping heartbeat for key %s: %s", key, hr);
                          }
                        } catch (RuntimeException e) {
                          warnThrottled(e, "Heartbeat failed; will retry on next interval");
                        }
                      });
            });
    rc.setProperty(PROP_IDEMPOTENCY_HEARTBEAT_TIMER_ID, timerId);
  }

  private String executorId() {
    String cached = resolvedExecutorId;
    if (cached != null) {
      return cached;
    }
    String fromConfig = configuration.executorId().orElse(null);
    if (fromConfig != null && !fromConfig.isBlank()) {
      resolvedExecutorId = fromConfig;
      return fromConfig;
    }
    String computed = defaultExecutorId();
    resolvedExecutorId = computed;
    return computed;
  }

  private static String defaultExecutorId() {
    String pid = String.valueOf(ProcessHandle.current().pid());
    String node =
        firstNonBlank(
            System.getenv("POD_NAME"), System.getenv("HOSTNAME"), System.getenv("NODE_NAME"));
    if (node != null) {
      return node + "-" + pid;
    }
    try {
      return InetAddress.getLocalHost().getHostName() + "-" + pid;
    } catch (Exception e) {
      return "pid-" + pid;
    }
  }

  private static String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }
    for (String v : values) {
      if (v != null && !v.isBlank()) {
        return v;
      }
    }
    return null;
  }

  private void stopHeartbeat(ContainerRequestContext request) {
    Object tid = request.getProperty(PROP_IDEMPOTENCY_HEARTBEAT_TIMER_ID);
    if (tid instanceof Long l) {
      vertx.cancelTimer(l);
    }
  }

  private IdempotencyConfiguration.Scope matchScope(ContainerRequestContext rc) {
    String method = rc.getMethod();
    String path = rc.getUriInfo().getPath();
    if (method == null || path == null) {
      return null;
    }
    String normalizedPath = path.startsWith("/") ? path.substring(1) : path;
    for (IdempotencyConfiguration.Scope s : configuration.scopes()) {
      if (s == null) {
        continue;
      }
      if (!method.equalsIgnoreCase(s.method())) {
        continue;
      }
      String prefix = s.pathPrefix();
      if (prefix == null || prefix.isBlank()) {
        continue;
      }
      String normalizedPrefix = prefix.startsWith("/") ? prefix.substring(1) : prefix;
      if (normalizedPath.startsWith(normalizedPrefix)) {
        return s;
      }
    }
    return null;
  }

  private static String normalizeOperationType(ContainerRequestContext rc) {
    // Fallback for when scopes are not configured: use the matched JAX-RS resource method
    // (stable across resource IDs), falling back to the HTTP method if unavailable.
    try {
      ResteasyReactiveRequestContext ctx = CurrentRequestManager.get();
      if (ctx != null) {
        ResteasyReactiveResourceInfo info = ctx.getResteasyReactiveResourceInfo();
        if (info != null) {
          Class<?> resourceClass = info.getResourceClass();
          Method resourceMethod = info.getResourceMethod();
          if (resourceClass != null && resourceMethod != null) {
            return resourceClass.getName() + "#" + resourceMethod.getName();
          }
        }
      }
    } catch (Throwable ignored) {
      // Best-effort; fall back below.
    }
    return rc.getMethod().toLowerCase(Locale.ROOT);
  }

  private static boolean shouldFinalize(int httpStatus) {
    // - Finalize & replay: 200, 201, 204, and deterministic terminal 4xx
    // - Never finalize (not stored/replayed): 401, 403, 408, 429, all 5xx
    if (httpStatus == 200 || httpStatus == 201 || httpStatus == 204) {
      return true;
    }
    if (httpStatus >= 400 && httpStatus < 500) {
      return !NEVER_FINALIZE_STATUSES.contains(httpStatus);
    }
    return false;
  }

  private static boolean shouldCancelInProgressReservation(int httpStatus) {
    return httpStatus >= 500 || NEVER_FINALIZE_STATUSES.contains(httpStatus);
  }

  private static void warnThrottled(Throwable t, String message) {
    long now = System.currentTimeMillis();
    long last = LAST_WARN_AT_MS.get();
    if (now - last >= WARN_THROTTLE_MS && LAST_WARN_AT_MS.compareAndSet(last, now)) {
      LOG.warn(message, t);
    } else {
      LOG.debug(message, t);
    }
  }

  /**
   * Normalize and validate the idempotency key.
   *
   * <p>Idempotency keys must be UUIDv7 in string form; normalizing to {@link UUID#toString()}
   * avoids accidental mismatches caused by casing.
   */
  private static String normalizeIdempotencyKey(String rawKey) {
    if (rawKey == null) {
      return null;
    }
    String trimmed = rawKey.trim();
    // Iceberg REST OpenAPI expects 36-char UUID strings.
    if (trimmed.length() != 36) {
      return null;
    }
    try {
      UUID uuid = UUID.fromString(trimmed);
      return uuid.version() == 7 ? uuid.toString() : null;
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private Response replayResponse(IdempotencyRecord existing) {
    Response.ResponseBuilder replay =
        Response.status(existing.httpStatus() == null ? 200 : existing.httpStatus());
    replay.header("X-Idempotency-Replayed", "true");
    applyReplayedHeaders(replay, existing.responseHeaders());
    if (existing.responseSummary() != null) {
      replay.entity(existing.responseSummary());
    }
    return replay.build();
  }

  private Uni<IdempotencyRecord> waitForFinalized(String realmId, String key, Instant deadline) {
    long initialMs = configuration.pollInitialDelay().toMillis();
    return pollUntilFinalized(realmId, key, deadline, initialMs);
  }

  private Uni<IdempotencyRecord> pollUntilFinalized(
      String realmId, String key, Instant deadline, long delayMs) {
    long maxMs = configuration.pollMaxDelay().toMillis();
    return Uni.createFrom()
        .item(() -> store.load(realmId, key))
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .onItem()
        .transformToUni(
            opt -> {
              if (opt.isPresent() && opt.get().isFinalized()) {
                return Uni.createFrom().item(opt.get());
              }
              if (!clock.instant().isBefore(deadline)) {
                return Uni.createFrom()
                    .failure(new TimeoutException("Timed out waiting for finalize"));
              }
              long nextDelayMs = Math.min(maxMs, delayMs * 2);
              long jitter = ThreadLocalRandom.current().nextLong(delayMs / 2);
              return Uni.createFrom()
                  .nullItem()
                  .onItem()
                  .delayIt()
                  .by(Duration.ofMillis(delayMs + jitter))
                  .onItem()
                  .transformToUni(
                      ignored -> pollUntilFinalized(realmId, key, deadline, nextDelayMs));
            });
  }

  private static boolean isMutatingMethod(String method) {
    if (method == null) {
      return false;
    }
    switch (method.toUpperCase(Locale.ROOT)) {
      case "POST":
      case "PUT":
      case "PATCH":
      case "DELETE":
        return true;
      default:
        return false;
    }
  }

  private static boolean isIcebergMutationEndpoint(ContainerRequestContext rc) {
    // Best-effort: rely on the matched JAX-RS resource method rather than path heuristics so we
    // only apply idempotency to endpoints implemented by the Iceberg REST catalog adapter. If the
    // matched resource/method isn't available, we return false (no idempotency).
    try {
      ResteasyReactiveRequestContext ctx = CurrentRequestManager.get();
      if (ctx == null) {
        return false;
      }
      ResteasyReactiveResourceInfo info = ctx.getResteasyReactiveResourceInfo();
      if (info == null) {
        return false;
      }
      if (info.getResourceClass() != IcebergCatalogAdapter.class) {
        return false;
      }
      Method m = info.getResourceMethod();
      if (m == null) {
        return false;
      }
      String name = m.getName();
      // Mutation endpoints that accept Idempotency-Key in the Iceberg REST API.
      return name.equals("createNamespace")
          || name.equals("dropNamespace")
          || name.equals("updateProperties")
          || name.equals("createTable")
          || name.equals("registerTable")
          || name.equals("updateTable")
          || name.equals("dropTable")
          || name.equals("renameTable")
          || name.equals("createView")
          || name.equals("replaceView")
          || name.equals("dropView")
          || name.equals("renameView")
          || name.equals("commitTransaction");
    } catch (Throwable ignored) {
      return false;
    }
  }

  /**
   * Returns true only for Polaris-managed (INTERNAL) catalogs for Iceberg REST routes.
   *
   * <p>This is a best-effort guard: if we can't determine catalog type, we default to "not
   * internal" and treat idempotency as a no-op.
   */
  private boolean isPolarisManagedInternalIcebergCatalogRequest(
      String path, PolarisPrincipal principal) {
    if (path == null || !path.startsWith("v1/")) {
      // Non-Iceberg REST paths: leave existing behavior unchanged (scopes/mutating method gating).
      return true;
    }

    // Expected Iceberg REST shape: v1/{prefix}/...
    String[] parts = path.split("/", 3);
    if (parts.length < 3) {
      return false;
    }
    String prefix = parts[1];
    if (prefix == null || prefix.isBlank()) {
      return false;
    }

    final String catalogName;
    try {
      catalogName = prefixParser.prefixToCatalogName(prefix);
    } catch (RuntimeException e) {
      LOG.debugf(e, "Failed to parse Iceberg REST catalog prefix for idempotency gating");
      return false;
    }

    final Resolver resolver;
    try {
      resolver = resolverFactory.createResolver(principal, catalogName);
    } catch (RuntimeException e) {
      LOG.debugf(e, "Failed to create resolver for idempotency internal-catalog gating");
      return false;
    }

    ResolverStatus status;
    try {
      status = resolver.resolveAll();
    } catch (RuntimeException e) {
      LOG.debugf(e, "Failed to resolve catalog for idempotency internal-catalog gating");
      return false;
    }
    if (!ResolverStatus.StatusEnum.SUCCESS.equals(status.getStatus())) {
      LOG.debugf(
          "Resolver status %s for idempotency internal-catalog gating; treating as non-internal",
          status.getStatus());
      return false;
    }

    ResolvedPolarisEntity resolved = resolver.getResolvedReferenceCatalog();
    CatalogEntity catalogEntity = resolved == null ? null : CatalogEntity.of(resolved.getEntity());
    return catalogEntity != null
        && Catalog.TypeEnum.INTERNAL.equals(catalogEntity.getCatalogType());
  }

  private static String boundedResponseSummary(
      ContainerResponseContext response, ObjectMapper objectMapper, int maxBytes) {
    Object entity = response.getEntity();
    if (entity == null) {
      return null;
    }
    String serialized;
    if (entity instanceof String string) {
      serialized = string;
    } else {
      try {
        serialized = objectMapper.writeValueAsString(entity);
      } catch (Exception e) {
        LOG.debug(
            "Failed to serialize response entity for idempotency; body will not be replayed", e);
        return null;
      }
    }
    if (maxBytes > 0 && serialized.length() > maxBytes) {
      LOG.warnf(
          "Response body (%d chars) exceeds responseSummaryMaxBytes (%d); "
              + "replay will return status and headers only",
          serialized.length(), maxBytes);
      return null;
    }
    return serialized;
  }

  private static void applyReplayedHeaders(
      Response.ResponseBuilder rb, Map<String, String> responseHeaders) {
    if (responseHeaders == null || responseHeaders.isEmpty()) {
      return;
    }
    responseHeaders.forEach(
        (k, v) -> {
          rb.header(k, v);
          if (k != null && v != null && "content-type".equalsIgnoreCase(k)) {
            // Ensure the JAX-RS response media type matches the replayed Content-Type.
            rb.type(v);
          }
        });
  }

  private static Map<String, String> headerSnapshot(
      ContainerResponseContext response, List<String> allowlist) {
    if (allowlist == null || allowlist.isEmpty()) {
      return null;
    }
    Map<String, String> out = new LinkedHashMap<>();
    for (String name : allowlist) {
      if (name == null || name.isBlank()) {
        continue;
      }
      Object v = response.getHeaders().getFirst(name);
      if (v != null) {
        out.put(name, String.valueOf(v));
      }
    }
    if (out.isEmpty()) {
      return null;
    }
    return out;
  }

  private static Response error(int status, String type, String message) {
    return Response.status(status)
        .type(MediaType.APPLICATION_JSON_TYPE)
        .entity(
            ErrorResponse.builder()
                .responseCode(status)
                .withType(type)
                .withMessage(message)
                .build())
        .build();
  }
}

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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.IdempotencyRecord;
import org.apache.polaris.core.persistence.IdempotencyPersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reusable helper for handler-level idempotency.
 *
 * <p>Encapsulates the parts that are independent of any specific handler method: idempotency-key
 * validation (UUID v7), principal/resource hashing, executor id resolution, polling for in-progress
 * duplicates, and thin wrappers around the {@link IdempotencyPersistence} reserve / cancel /
 * finalize operations.
 *
 * <p>All configuration (whether the feature is on, header name, executor identity, TTLs,
 * in-progress wait, lease TTL) comes from {@link IdempotencyConfiguration} via CDI as a single
 * deployment-wide source. Per-realm or per-catalog overrides are intentionally not modelled in this
 * iteration; if operators need them later they can be added without changing handler-side call
 * shapes.
 *
 * <p>The actual persistence is sourced per-realm from {@link
 * MetaStoreManagerFactory#getOrCreateIdempotencyPersistence(org.apache.polaris.core.context.RealmContext)},
 * which each backend implements independently of {@link
 * org.apache.polaris.core.persistence.BasePersistence}.
 *
 * <p>The handler decides what to do on each {@link Outcome}: for {@link Outcome#owned()} it
 * executes the operation and finalizes; for {@link Outcome#duplicate(IdempotencyRecord)} it
 * rebuilds the response from authoritative state (no stored response replay).
 */
@ApplicationScoped
public class IdempotencyHandlerSupport {

  private static final Logger LOGGER = LoggerFactory.getLogger(IdempotencyHandlerSupport.class);

  // RFC 9562 UUID v7 has version nibble 7 in time_hi_and_version.
  private static final Pattern UUID_V7_PATTERN =
      Pattern.compile(
          "^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
          Pattern.CASE_INSENSITIVE);

  @Inject IdempotencyConfiguration configuration;
  @Inject MetaStoreManagerFactory metaStoreManagerFactory;
  @Inject Clock clock;

  private final AtomicReference<String> resolvedExecutorId = new AtomicReference<>();

  /**
   * Test-only factory that builds an instance without going through CDI. Tests pass a {@code
   * persistenceLookup} function returning the {@link IdempotencyPersistence} for a given realm id;
   * production code goes through {@link MetaStoreManagerFactory#getOrCreateSession}.
   */
  public static IdempotencyHandlerSupport forTesting(
      IdempotencyConfiguration configuration,
      Function<String, IdempotencyPersistence> persistenceLookup,
      Clock clock) {
    IdempotencyHandlerSupport support = new IdempotencyHandlerSupport();
    support.configuration = configuration;
    support.clock = clock;
    support.testPersistenceLookup = persistenceLookup;
    return support;
  }

  // Test-only override; null in production.
  private Function<String, IdempotencyPersistence> testPersistenceLookup;

  /** Returns true if handler-level idempotency is enabled. */
  public boolean isEnabled() {
    return configuration.enabled();
  }

  /**
   * Reads and validates the idempotency key from the JAX-RS request headers using the deploy-time
   * configured header name from {@link IdempotencyConfiguration#keyHeader()}.
   *
   * @return validated key, or {@link Optional#empty()} if {@code httpHeaders} is null, the header
   *     is absent / blank, or idempotency is disabled
   * @throws IllegalArgumentException if the header is present but not a valid UUID v7 (callers
   *     translate this into a 400 Bad Request)
   */
  public Optional<String> validatedKey(@Nullable HttpHeaders httpHeaders) {
    if (httpHeaders == null) {
      return Optional.empty();
    }
    return validatedKey(httpHeaders.getHeaderString(configuration.keyHeader()));
  }

  /**
   * Returns the active idempotency key for this request, or {@link Optional#empty()} if idempotency
   * is disabled, the header is absent or blank, or the value fails validation.
   *
   * <p>Validation rejects keys that are not UUID v7. Strict validation prevents low-entropy or
   * easily-guessable identifiers from becoming a security boundary.
   *
   * @throws IllegalArgumentException if the key is present but malformed (handlers translate this
   *     into a 400 Bad Request)
   */
  public Optional<String> validatedKey(@Nullable String headerValue) {
    if (!isEnabled() || headerValue == null) {
      return Optional.empty();
    }
    String trimmed = headerValue.trim();
    if (trimmed.isEmpty()) {
      return Optional.empty();
    }
    if (!UUID_V7_PATTERN.matcher(trimmed).matches()) {
      throw new IllegalArgumentException(
          "Idempotency-Key must be a UUID v7; got: " + summarizeKey(trimmed));
    }
    // Validate parseability as well to reject stray formatting.
    try {
      UUID.fromString(trimmed);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Idempotency-Key must be a valid UUID");
    }
    return Optional.of(trimmed.toLowerCase(Locale.ROOT));
  }

  /**
   * SHA-256 (hex) of the calling principal's identity, bound to the realm. The input is the
   * principal name, the realm id, and the activated role set, canonicalised so the hash is
   * deterministic and order-independent.
   *
   * <p>Roles are part of the binding so two callers that share a name but differ in activated roles
   * do not collide. Principal properties are intentionally excluded: they are admin-mutable and not
   * authentication context, so unrelated property edits (SCIM sync, display-name update, tagging)
   * during the idempotency TTL would otherwise invalidate retries from the same client. The
   * principal's access token is also excluded because tokens rotate without changing identity.
   */
  public String principalHash(PolarisPrincipal principal, String realmId) {
    StringBuilder sb = new StringBuilder();
    sb.append("name=").append(principal.getName()).append('|');
    sb.append("realm=").append(realmId).append('|');
    sb.append("roles=");
    new TreeSet<>(principal.getRoles()).forEach(r -> sb.append(r).append(','));
    return sha256Hex(sb.toString());
  }

  /** SHA-256 hex of an arbitrary string used as the resource binding component. */
  public String resourceHash(String value) {
    return sha256Hex(value);
  }

  /**
   * Attempt to acquire the idempotency key, returning the outcome the handler must dispatch on.
   *
   * <p>If the existing reservation belongs to a different principal or different normalized
   * resource, this method throws {@link ConflictException}, which the handler translates to 422.
   *
   * <p>If the existing reservation is still in progress and not stale, this method blocks (with a
   * short polling interval) up to the configured in-progress wait, then either returns the
   * duplicate (now finalized) or throws {@link InProgressTimeoutException} which the handler
   * translates to 409 (retry later). TTL, in-progress wait, and lease TTL come from {@link
   * IdempotencyConfiguration}.
   */
  public Outcome reserveOrWait(
      String realmId,
      String idempotencyKey,
      String operationType,
      String normalizedResourceId,
      String principalHash) {
    IdempotencyPersistence persistence = persistenceFor(realmId);
    Instant now = clock.instant();
    Instant expiresAt = now.plus(configuration.ttl()).plus(configuration.ttlGrace());
    String executorId = resolveExecutorId();

    IdempotencyPersistence.ReserveResult result =
        persistence.reserve(
            realmId,
            idempotencyKey,
            operationType,
            normalizedResourceId,
            principalHash,
            expiresAt,
            executorId,
            now);

    if (result.type() == IdempotencyPersistence.ReserveResultType.OWNED) {
      return Outcome.owned(executorId);
    }

    IdempotencyRecord existing =
        result.existing().orElseThrow(() -> new IllegalStateException("DUPLICATE without record"));
    return validateAndWait(
        persistence,
        realmId,
        idempotencyKey,
        operationType,
        normalizedResourceId,
        principalHash,
        existing);
  }

  /**
   * Marks an owned reservation as finalized with the given HTTP status. Used by handlers after
   * successful completion.
   *
   * <p>{@code responseSummary} is always {@code null} for credential-bearing mutations (which
   * re-derive the response on replay).
   */
  public void finalizeOwned(
      String realmId,
      String idempotencyKey,
      String executorId,
      int httpStatus,
      @Nullable String errorSubtype,
      @Nullable String responseSummary) {
    IdempotencyPersistence persistence = persistenceFor(realmId);
    boolean ok =
        persistence.finalizeRecord(
            realmId,
            idempotencyKey,
            executorId,
            httpStatus,
            errorSubtype,
            responseSummary,
            clock.instant());
    if (!ok) {
      // Log a redacted key summary (first/last 4 chars) so DEBUG output is useful for correlation
      // without leaking the full client-supplied Idempotency-Key into log pipelines.
      LOGGER.debug(
          "finalizeRecord returned false for realm={} key={} (race with another finalize?)",
          realmId,
          summarizeKey(idempotencyKey));
    }
  }

  /**
   * Cancels an owned in-progress reservation when the handler hits an error before reaching
   * finalization (for example a 4xx auth failure or an IAM error during credential vending).
   * Subsequent retries are then free to reacquire the same key.
   */
  public void cancelOwned(String realmId, String idempotencyKey, String executorId) {
    IdempotencyPersistence persistence = persistenceFor(realmId);
    boolean cancelled =
        persistence.cancelInProgressReservation(realmId, idempotencyKey, executorId);
    if (!cancelled) {
      // Same redaction as in finalizeOwned: keep the key out of DEBUG log output verbatim.
      LOGGER.debug(
          "cancelInProgressReservation returned false for realm={} key={} executor={}",
          realmId,
          summarizeKey(idempotencyKey),
          executorId);
    }
  }

  /** Returns the executor id used to tag reservations on this node. Cached after first call. */
  public String resolveExecutorId() {
    String cached = resolvedExecutorId.get();
    if (cached != null) {
      return cached;
    }
    String resolved = resolveExecutorId(configuration);
    resolvedExecutorId.compareAndSet(null, resolved);
    return resolvedExecutorId.get();
  }

  static String resolveExecutorId(IdempotencyConfiguration configuration) {
    String fromConfig = configuration.executorId().orElse(null);
    if (fromConfig != null && !fromConfig.isBlank()) {
      return fromConfig;
    }
    return defaultExecutorId();
  }

  /**
   * Resolves the per-realm {@link IdempotencyPersistence}. In production this delegates to {@link
   * MetaStoreManagerFactory#getOrCreateIdempotencyPersistence}; in unit tests it goes through the
   * lookup function passed to {@link #forTesting}.
   */
  IdempotencyPersistence persistenceFor(String realmId) {
    if (testPersistenceLookup != null) {
      return testPersistenceLookup.apply(realmId);
    }
    return metaStoreManagerFactory.getOrCreateIdempotencyPersistence(() -> realmId);
  }

  private Outcome validateAndWait(
      IdempotencyPersistence persistence,
      String realmId,
      String idempotencyKey,
      String expectedOperationType,
      String expectedResourceId,
      String expectedPrincipalHash,
      IdempotencyRecord existing) {
    if (!expectedPrincipalHash.equals(existing.principalHash())) {
      throw new ConflictException(
          "Idempotency-Key already used by a different caller for the same key");
    }
    if (!expectedResourceId.equals(existing.normalizedResourceId())
        || !expectedOperationType.equals(existing.operationType())) {
      throw new ConflictException(
          "Idempotency-Key already used for a different operation/resource");
    }

    if (existing.isFinalized()) {
      return Outcome.duplicate(existing);
    }

    Duration maxWait = configuration.inProgressWait();
    Duration interval = configuration.inProgressPollInterval();
    Duration leaseTtl = configuration.leaseTtl();

    Instant deadline = clock.instant().plus(maxWait);
    IdempotencyRecord current = existing;
    while (true) {
      if (current.isFinalized()) {
        return Outcome.duplicate(current);
      }
      // If the existing owner appears stale (no recent heartbeat), don't block forever.
      Instant heartbeat =
          current.heartbeatAt() != null ? current.heartbeatAt() : current.createdAt();
      if (heartbeat.plus(leaseTtl).isBefore(clock.instant())) {
        throw new InProgressTimeoutException(
            "In-progress reservation appears stale (no heartbeat within lease TTL)");
      }
      if (clock.instant().isAfter(deadline)) {
        throw new InProgressTimeoutException(
            "Timed out waiting for in-progress idempotency key to finalize");
      }
      try {
        Thread.sleep(Math.max(1L, interval.toMillis()));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for idempotency key", e);
      }
      current =
          persistence
              .loadIdempotencyRecord(realmId, idempotencyKey)
              .orElseThrow(() -> new IllegalStateException("In-progress record disappeared"));
    }
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

  private static String sha256Hex(@Nonnull String input) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder(digest.length * 2);
      for (byte b : digest) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  private static String summarizeKey(String key) {
    if (key.length() <= 8) {
      return key;
    }
    return key.substring(0, 4) + "..." + key.substring(key.length() - 4);
  }

  /** Result of {@link #reserveOrWait}. */
  public sealed interface Outcome permits Outcome.Owned, Outcome.Duplicate {

    static Outcome owned(String executorId) {
      return new Owned(executorId);
    }

    static Outcome duplicate(IdempotencyRecord existing) {
      return new Duplicate(existing);
    }

    /** This caller owns the reservation and must execute the operation. */
    record Owned(String executorId) implements Outcome {}

    /** Another caller already finalized this reservation. Handler must rebuild the response. */
    record Duplicate(IdempotencyRecord existing) implements Outcome {}
  }

  /** Thrown when the idempotency key is reused with a different binding. Maps to HTTP 422. */
  public static final class ConflictException extends RuntimeException {
    public ConflictException(String message) {
      super(message);
    }
  }

  /** Thrown when waiting for an in-progress duplicate timed out. Maps to HTTP 409 retry-later. */
  public static final class InProgressTimeoutException extends RuntimeException {
    public InProgressTimeoutException(String message) {
      super(message);
    }
  }
}

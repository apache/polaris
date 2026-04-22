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
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.apache.polaris.core.entity.IdempotencyRecord;
import org.apache.polaris.core.persistence.IdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reusable helper for handler-level idempotency.
 *
 * <p>Encapsulates the parts that are independent of any specific handler method: idempotency-key
 * validation (UUID v7), principal/resource hashing, executor id resolution, polling for in-progress
 * duplicates, and thin wrappers around the {@link IdempotencyStore} reserve / cancel / finalize
 * operations.
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
  @Inject Instance<IdempotencyStore> storeInstance;
  @Inject Clock clock;

  private final AtomicReference<String> resolvedExecutorId = new AtomicReference<>();

  /**
   * Test-only factory that builds an instance without going through CDI. Intended for unit tests
   * that need to wire a mock {@link IdempotencyStore}.
   */
  public static IdempotencyHandlerSupport forTesting(
      IdempotencyConfiguration configuration,
      Instance<IdempotencyStore> storeInstance,
      Clock clock) {
    IdempotencyHandlerSupport support = new IdempotencyHandlerSupport();
    support.configuration = configuration;
    support.storeInstance = storeInstance;
    support.clock = clock;
    return support;
  }

  /** Returns true if handler-level idempotency is enabled and the store is wired. */
  public boolean isEnabled() {
    return configuration.enabled() && !storeInstance.isUnsatisfied();
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
   * SHA-256 of {@code principalName + ":" + realmId}, returned as a hex string. The input is
   * deterministic per (principal, realm) and is what the store binds against.
   */
  public String principalHash(String principalName, String realmId) {
    return sha256Hex(principalName + ":" + realmId);
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
   * short polling interval) up to {@link IdempotencyConfiguration#inProgressWait()}, then either
   * returns the duplicate (now finalized) or throws {@link InProgressTimeoutException} which the
   * handler translates to 409 (retry later).
   */
  public Outcome reserveOrWait(
      String realmId,
      String idempotencyKey,
      String operationType,
      String normalizedResourceId,
      String principalHash) {
    IdempotencyStore store = storeInstance.get();
    Instant now = clock.instant();
    Instant expiresAt = now.plus(configuration.ttl()).plus(configuration.ttlGrace());
    String executorId = resolveExecutorId();

    IdempotencyStore.ReserveResult result =
        store.reserve(
            realmId,
            idempotencyKey,
            operationType,
            normalizedResourceId,
            principalHash,
            expiresAt,
            executorId,
            now);

    if (result.type() == IdempotencyStore.ReserveResultType.OWNED) {
      return Outcome.owned(executorId);
    }

    IdempotencyRecord existing =
        result.existing().orElseThrow(() -> new IllegalStateException("DUPLICATE without record"));
    return validateAndWait(
        store,
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
    IdempotencyStore store = storeInstance.get();
    boolean ok =
        store.finalizeRecord(
            realmId,
            idempotencyKey,
            executorId,
            httpStatus,
            errorSubtype,
            responseSummary,
            clock.instant());
    if (!ok) {
      LOGGER.debug(
          "finalizeRecord returned false for realm={} key={} (race with another finalize?)",
          realmId,
          idempotencyKey);
    }
  }

  /**
   * Cancels an owned in-progress reservation when the handler hits an error before reaching
   * finalization (for example a 4xx auth failure or an IAM error during credential vending).
   * Subsequent retries are then free to reacquire the same key.
   */
  public void cancelOwned(String realmId, String idempotencyKey, String executorId) {
    IdempotencyStore store = storeInstance.get();
    boolean cancelled = store.cancelInProgressReservation(realmId, idempotencyKey, executorId);
    if (!cancelled) {
      LOGGER.debug(
          "cancelInProgressReservation returned false for realm={} key={} executor={}",
          realmId,
          idempotencyKey,
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

  private Outcome validateAndWait(
      IdempotencyStore store,
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
          store
              .load(realmId, idempotencyKey)
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

  /** Result of {@link #reserveOrWait(String, String, String, String, String)}. */
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

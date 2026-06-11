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

import com.google.common.hash.Hashing;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Locale;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.IdempotencyRecord;
import org.apache.polaris.core.persistence.IdempotencyStore;
import org.apache.polaris.core.persistence.IdempotencyStoreFactory;

/**
 * Handler-side helper for the single-transaction ("optimistic commit") idempotency model.
 *
 * <p>Responsibilities:
 *
 * <ul>
 *   <li>Read and validate the {@code Idempotency-Key} request header (UUIDv7 only).
 *   <li>Compute the principal/resource hashes that form the binding stored alongside each record.
 *   <li>Pre-flight: look up an existing record for the same {@code (realm, key)} and dispatch into
 *       {@link Outcome#owned()} or {@link Outcome#duplicate(IdempotencyRecord)} for the handler.
 *   <li>Record the terminal outcome after a successful operation, returning {@link Outcome#owned()}
 *       on win and {@link Outcome#duplicate(IdempotencyRecord)} on a race-driven duplicate.
 * </ul>
 *
 * <p>This bean is {@link RequestScoped}: a single request operates within one realm, so the
 * realm-scoped {@link IdempotencyStore} is resolved once (lazily) from {@link
 * IdempotencyStoreFactory} for the request's {@link RealmContext}. When idempotency is disabled the
 * store is never resolved — the bean is an inert shell. No response body is stored; duplicate
 * responses are rebuilt from authoritative catalog state by the handler itself.
 */
@RequestScoped
public class IdempotencyHandlerSupport {

  // RFC 9562 UUID v7 has version nibble 7 in time_hi_and_version.
  private static final Pattern UUID_V7_PATTERN =
      Pattern.compile(
          "^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
          Pattern.CASE_INSENSITIVE);

  @Inject IdempotencyConfiguration configuration;
  @Inject IdempotencyStoreFactory storeFactory;
  @Inject RealmContext realmContext;
  @Inject Clock clock;

  // Resolved lazily on first use within the request; never resolved when idempotency is disabled.
  private IdempotencyStore store;

  /**
   * Returns an instance with idempotency permanently disabled. Useful for test fixtures that need a
   * non-null {@code IdempotencyHandlerSupport} but exercise non-idempotent code paths.
   */
  public static IdempotencyHandlerSupport disabled() {
    IdempotencyHandlerSupport instance = new IdempotencyHandlerSupport();
    instance.configuration = DisabledConfiguration.INSTANCE;
    return instance;
  }

  /** Returns {@code true} if handler-level idempotency is enabled. */
  public boolean isEnabled() {
    return configuration != null && configuration.enabled();
  }

  /**
   * Reads and validates the idempotency key from the request headers using the deploy-time
   * configured header name from {@link IdempotencyConfiguration#keyHeader()}.
   *
   * @return validated key, or {@link Optional#empty()} if {@code httpHeaders} is null, the header
   *     is absent / blank, or idempotency is disabled
   * @throws IllegalArgumentException if the header is present but not a valid UUIDv7 (callers
   *     translate this into a 400 Bad Request)
   */
  public Optional<String> validatedKey(@Nullable HttpHeaders httpHeaders) {
    if (httpHeaders == null) {
      return Optional.empty();
    }
    return validatedKey(httpHeaders.getHeaderString(configuration.keyHeader()));
  }

  /**
   * Validates a raw header value. UUIDv7 is required so that the key has enough entropy to be a
   * meaningful idempotency boundary.
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
          "Idempotency-Key must be a UUIDv7; got: " + summarizeKey(trimmed));
    }
    try {
      UUID.fromString(trimmed);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Idempotency-Key must be a valid UUID");
    }
    return Optional.of(trimmed.toLowerCase(Locale.ROOT));
  }

  /**
   * SHA-256 (hex) of the calling principal's identity, bound to the request's realm. The input is
   * the principal name, the realm id, and the activated role set, canonicalised so the hash is
   * deterministic and order-independent.
   *
   * <p>Roles are part of the binding so two callers that share a name but differ in activated roles
   * do not collide. Principal properties are intentionally excluded: they are admin-mutable and not
   * authentication context.
   */
  public String principalHash(PolarisPrincipal principal) {
    StringBuilder sb = new StringBuilder();
    sb.append("name=").append(principal.getName()).append('|');
    sb.append("realm=").append(realmContext.getRealmIdentifier()).append('|');
    sb.append("roles=");
    new TreeSet<>(principal.getRoles()).forEach(r -> sb.append(r).append(','));
    return sha256Hex(sb.toString());
  }

  /**
   * SHA-256 hex of the resource-binding component, built from the operation and its stable resource
   * identity components (e.g. namespace, name, resolved access-delegation mode). The canonical
   * format lives here so all binding/hashing logic stays in one place; callers only supply the
   * identity components. The request payload itself is intentionally not part of the binding.
   */
  public String resourceHash(IdempotentOperation operation, String... components) {
    StringBuilder sb = new StringBuilder(operation.wireName());
    for (String component : components) {
      sb.append(':').append(component == null ? "" : component);
    }
    return sha256Hex(sb.toString());
  }

  /**
   * Pre-flight: look up an existing record for {@code (realm, key)} and dispatch the handler.
   *
   * <p>If an existing record is found, the binding is validated against the current request and a
   * mismatch raises {@link IdempotencyConflictException} (mapped to HTTP 422). If the binding
   * matches, the existing record is returned as a {@link Outcome.Duplicate} so the handler can
   * rebuild the response from current catalog state.
   *
   * <p>If no record exists, this returns {@link Outcome.Owned}; the handler should perform the
   * operation and then call {@link #recordOutcome(IdempotentOperation, String, String, String, int,
   * String)} to commit a record.
   */
  public Outcome preflight(
      String idempotencyKey,
      IdempotentOperation operation,
      String resourceHash,
      String principalHash) {
    Optional<IdempotencyRecord> existing = store().load(idempotencyKey);
    if (existing.isEmpty()) {
      return Outcome.owned();
    }
    return matchOrConflict(existing.get(), operation, resourceHash, principalHash);
  }

  /**
   * Records the terminal outcome of an operation that just completed.
   *
   * <p>The insert is atomic on {@code (realm, key)}; if another caller raced ahead and inserted
   * first, the returned {@link Outcome} carries that existing record so the handler can rebuild an
   * equivalent response from current state.
   */
  public Outcome recordOutcome(
      String idempotencyKey,
      IdempotentOperation operation,
      String resourceHash,
      String principalHash,
      int httpStatus,
      @Nullable String metadataLocation) {
    Instant now = clock.instant();
    Instant expiresAt = now.plus(configuration.ttl());
    IdempotencyStore.RecordResult result =
        store()
            .recordIfAbsent(
                idempotencyKey,
                operation.wireName(),
                resourceHash,
                principalHash,
                httpStatus,
                metadataLocation,
                now,
                expiresAt);
    if (result.type() == IdempotencyStore.RecordResultType.OWNED) {
      return Outcome.owned();
    }
    IdempotencyRecord existing =
        result
            .existing()
            .orElseThrow(() -> new IllegalStateException("DUPLICATE result without record"));
    return matchOrConflict(existing, operation, resourceHash, principalHash);
  }

  /**
   * Resolves (once per request) the realm-scoped {@link IdempotencyStore} for the request's realm.
   * Only called from the idempotency code paths, so it is never invoked when the feature is
   * disabled.
   */
  private IdempotencyStore store() {
    if (store == null) {
      store = storeFactory.getOrCreateIdempotencyStore(realmContext);
    }
    return store;
  }

  private static Outcome matchOrConflict(
      IdempotencyRecord existing,
      IdempotentOperation expectedOperation,
      String expectedResourceHash,
      String expectedPrincipalHash) {
    if (!expectedPrincipalHash.equals(existing.principalHash())
        || !expectedResourceHash.equals(existing.resourceHash())
        || !expectedOperation.wireName().equals(existing.operationType())) {
      throw new IdempotencyConflictException(
          "Idempotency-Key already used with a different binding (caller, operation, or resource)");
    }
    return Outcome.duplicate(existing);
  }

  private static String sha256Hex(@Nonnull String input) {
    return Hashing.sha256().hashString(input, StandardCharsets.UTF_8).toString();
  }

  private static String summarizeKey(String key) {
    if (key.length() <= 8) {
      return key;
    }
    return key.substring(0, 4) + "..." + key.substring(key.length() - 4);
  }

  /** Result of {@link #preflight} and {@link #recordOutcome}. */
  public sealed interface Outcome permits Outcome.Owned, Outcome.Duplicate {

    static Outcome owned() {
      return Owned.INSTANCE;
    }

    static Outcome duplicate(IdempotencyRecord existing) {
      return new Duplicate(existing);
    }

    /** This caller may proceed with the operation (no prior record found, or insert succeeded). */
    final class Owned implements Outcome {
      private static final Owned INSTANCE = new Owned();

      private Owned() {}
    }

    /** Another caller already recorded an outcome for this key. Handler must rebuild a response. */
    record Duplicate(IdempotencyRecord existing) implements Outcome {}
  }

  /** Minimal {@link IdempotencyConfiguration} returning {@code enabled=false}; used by tests. */
  private static final class DisabledConfiguration implements IdempotencyConfiguration {
    static final DisabledConfiguration INSTANCE = new DisabledConfiguration();

    @Override
    public boolean enabled() {
      return false;
    }

    @Override
    public String type() {
      return "in-memory";
    }

    @Override
    public String keyHeader() {
      return "Idempotency-Key";
    }

    @Override
    public java.time.Duration ttl() {
      return java.time.Duration.ofMinutes(5);
    }

    @Override
    public boolean purgeEnabled() {
      return false;
    }

    @Override
    public java.time.Duration purgeInterval() {
      return java.time.Duration.ofDays(1);
    }
  }
}

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

import jakarta.annotation.Nullable;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.polaris.core.DigestUtils;
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
 *   <li>Pre-flight: from the raw request inputs, decide whether idempotency applies and, if so,
 *       look up an existing record for the same {@code (realm, key)}, returning an {@link
 *       IdempotencyOutcome} for the handler to branch on.
 *   <li>Record the terminal outcome after a successful operation, returning {@link
 *       IdempotencyOutcome.Owned} on win and {@link IdempotencyOutcome.Duplicate} on a race-driven
 *       duplicate.
 *   <li>Resolve a concurrent create-table race by polling for the winner's record.
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

  // Bounded lookup for the idempotency record written by a concurrent create-table race winner,
  // which records only after committing the table (so a loser may briefly not see it yet). Backoff
  // is exponential: 5+10+20+40+80 = 155ms total budget across 5 attempts. If the winner's record is
  // still not visible after that (e.g. a long GC pause or slow store write), the original 409
  // surfaces instead of a replay — correct but not ideal; widen the budget if this proves too
  // tight.
  private static final int CONCURRENT_REPLAY_MAX_ATTEMPTS = 5;
  private static final long CONCURRENT_REPLAY_INITIAL_BACKOFF_MILLIS = 5;

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
   * Validates that the {@code Idempotency-Key} is a UUIDv7. UUIDv7 is required so the key carries a
   * timestamp and enough entropy to be a meaningful idempotency boundary. The header is already
   * parsed into a {@link UUID} upstream (the JAX-RS binding), so this only enforces the version.
   *
   * @return the key, or {@link Optional#empty()} if {@code idempotencyKey} is null or idempotency
   *     is disabled
   * @throws IllegalArgumentException if the key is not a UUIDv7 (callers translate this into a 400
   *     Bad Request)
   */
  public Optional<UUID> validatedKey(@Nullable UUID idempotencyKey) {
    if (!isEnabled() || idempotencyKey == null) {
      return Optional.empty();
    }
    // RFC 9562 UUIDv7: version 7 with the RFC 4122/9562 variant (2).
    if (idempotencyKey.version() != 7 || idempotencyKey.variant() != 2) {
      throw new IllegalArgumentException(
          "Idempotency-Key must be a UUIDv7; got: " + idempotencyKey);
    }
    return Optional.of(idempotencyKey);
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
    return DigestUtils.sha256Hex(sb.toString());
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
    return DigestUtils.sha256Hex(sb.toString());
  }

  /**
   * Pre-flight: decide whether idempotency applies to this request and, if so, look up an existing
   * record for {@code (realm, key)}.
   *
   * <p>All of the work lives here: if idempotency is disabled or no key was supplied, this returns
   * {@link IdempotencyOutcome.Disabled} and the handler runs its plain path. Otherwise the
   * principal/resource hashes are computed from the raw inputs and the store is consulted:
   *
   * <ul>
   *   <li>No record → {@link IdempotencyOutcome.Owned} (carrying the computed binding); the handler
   *       performs the operation and then calls {@link #recordOutcome}.
   *   <li>Matching record → {@link IdempotencyOutcome.Duplicate}; the handler rebuilds the response
   *       from current catalog state.
   *   <li>Record with a different binding → {@link IdempotencyConflictException} (mapped to 422).
   * </ul>
   *
   * @param idempotencyKey the validated key, or {@link Optional#empty()} when absent/disabled
   * @param principal the calling principal (hashed into the binding)
   * @param operation the idempotent operation being attempted
   * @param resourceComponents the stable resource-identity components (e.g. namespace, name,
   *     resolved access-delegation mode)
   */
  public IdempotencyOutcome preflight(
      Optional<UUID> idempotencyKey,
      PolarisPrincipal principal,
      IdempotentOperation operation,
      String... resourceComponents) {
    if (!isEnabled() || idempotencyKey.isEmpty()) {
      return IdempotencyOutcome.disabled();
    }
    IdempotencyOutcome.Owned owned =
        IdempotencyOutcome.owned(
            idempotencyKey.get(),
            operation,
            resourceHash(operation, resourceComponents),
            principalHash(principal));
    return lookup(owned);
  }

  /**
   * Records the terminal outcome of an operation that just completed, using the binding carried by
   * the {@link IdempotencyOutcome.Owned} returned from {@link #preflight}.
   *
   * <p>The insert is atomic on {@code (realm, key)}; if another caller raced ahead and inserted
   * first, this returns {@link IdempotencyOutcome.Duplicate} carrying that existing record so the
   * handler can rebuild an equivalent response from current state. Otherwise the same {@code owned}
   * outcome is returned to signal a clean win.
   */
  public IdempotencyOutcome recordOutcome(
      IdempotencyOutcome.Owned owned, int httpStatus, @Nullable String metadataLocation) {
    Instant now = clock.instant();
    Instant expiresAt = now.plus(configuration.ttl());
    IdempotencyStore.RecordResult result =
        store()
            .recordIfAbsent(
                owned.idempotencyKey(),
                owned.operation().wireName(),
                owned.resourceHash(),
                owned.principalHash(),
                httpStatus,
                metadataLocation,
                now,
                expiresAt);
    if (result.type() == IdempotencyStore.RecordResultType.OWNED) {
      return owned;
    }
    IdempotencyRecord existing =
        result
            .existing()
            .orElseThrow(() -> new IllegalStateException("DUPLICATE result without record"));
    return matchOrConflict(existing, owned);
  }

  /**
   * After a concurrent {@code createTable} loses the catalog race (AlreadyExistsException), checks
   * whether the race winner recorded a matching idempotency outcome so this caller can replay it.
   *
   * <p>The winner records its outcome only after committing the table, so a loser may observe the
   * conflict slightly before the record is visible; this polls a bounded number of times. A binding
   * mismatch surfaces as {@link IdempotencyConflictException} (mapped to 422). Returns empty if no
   * matching record appears, meaning the conflict was a genuine pre-existing table rather than a
   * same-key retry.
   */
  public Optional<IdempotencyRecord> resolveConcurrentDuplicate(IdempotencyOutcome.Owned owned) {
    long backoffMillis = CONCURRENT_REPLAY_INITIAL_BACKOFF_MILLIS;
    for (int attempt = 0; attempt < CONCURRENT_REPLAY_MAX_ATTEMPTS; attempt++) {
      if (lookup(owned) instanceof IdempotencyOutcome.Duplicate dup) {
        return Optional.of(dup.existing());
      }
      try {
        Thread.sleep(backoffMillis);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        break;
      }
      backoffMillis *= 2;
    }
    return Optional.empty();
  }

  /** Single store lookup: returns {@code owned} if no record exists, else matches or conflicts. */
  private IdempotencyOutcome lookup(IdempotencyOutcome.Owned owned) {
    Optional<IdempotencyRecord> existing = store().load(owned.idempotencyKey());
    if (existing.isEmpty()) {
      return owned;
    }
    return matchOrConflict(existing.get(), owned);
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

  private static IdempotencyOutcome matchOrConflict(
      IdempotencyRecord existing, IdempotencyOutcome.Owned expected) {
    if (!expected.principalHash().equals(existing.principalHash())
        || !expected.resourceHash().equals(existing.resourceHash())
        || !expected.operation().wireName().equals(existing.operationType())) {
      throw new IdempotencyConflictException(
          "Idempotency-Key already used with a different binding (caller, operation, or resource)");
    }
    return IdempotencyOutcome.duplicate(existing);
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

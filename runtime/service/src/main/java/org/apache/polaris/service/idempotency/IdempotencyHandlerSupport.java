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

/**
 * Handler-side helper for the single-transaction ("optimistic commit") idempotency model.
 *
 * <p>Responsibilities:
 *
 * <ul>
 *   <li>Read and validate the {@code Idempotency-Key} request header (UUIDv7 only).
 *   <li>Compute the single binding hash (caller, operation, resource) stored alongside each record.
 *   <li>Pre-flight: from the raw request inputs, decide whether idempotency applies and, if so,
 *       look up an existing record for the same {@code (realm, key)}, returning an {@link
 *       IdempotencyOutcome} for the handler to branch on.
 *   <li>Record the terminal outcome after a successful operation, returning the {@link
 *       IdempotencyOutcome.New} outcome on win and {@link IdempotencyOutcome.Duplicate} on a
 *       race-driven duplicate.
 *   <li>Resolve a concurrent create-table race by polling for the winner's record.
 * </ul>
 *
 * <p>This bean is {@link RequestScoped}: a single request operates within one realm, so the
 * injected {@link IdempotencyStore} is already bound to the request's realm (see {@link
 * IdempotencyStoreProducer}). When idempotency is disabled the store is a {@link
 * NoOpIdempotencyStore} and is never touched. No response body is stored; duplicate responses are
 * rebuilt from authoritative catalog state by the handler itself.
 */
@RequestScoped
public class IdempotencyHandlerSupport {

  @Inject IdempotencyConfiguration configuration;
  @Inject IdempotencyStore store;
  @Inject RealmContext realmContext;
  @Inject Clock clock;

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
    return configuration.enabled();
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
   * SHA-256 (hex) of the full binding for a request: the calling principal's identity (name, realm,
   * activated roles), the operation, and the operation's stable resource-identity components (e.g.
   * namespace, name, resolved access-delegation mode). Folding everything into one hash is
   * sufficient for replay detection — the handler only ever needs to know whether the binding
   * matches, not which component differs.
   *
   * <p>The canonical format lives here so all binding/hashing logic stays in one place; callers
   * only supply the identity components. Roles are included so two callers that share a name but
   * differ in activated roles do not collide. Principal properties are intentionally excluded
   * (admin-mutable and not authentication context), and the request payload is intentionally not
   * part of the binding.
   */
  public String bindingHash(
      IdempotentOperation operation, PolarisPrincipal principal, String... resourceComponents) {
    StringBuilder sb = new StringBuilder();
    sb.append("op=").append(operation.wireName()).append('|');
    sb.append("name=").append(principal.getName()).append('|');
    sb.append("realm=").append(realmContext.getRealmIdentifier()).append('|');
    sb.append("roles=");
    new TreeSet<>(principal.getRoles()).forEach(r -> sb.append(r).append(','));
    sb.append("|resource=");
    for (String component : resourceComponents) {
      sb.append(component == null ? "" : component).append(':');
    }
    return DigestUtils.sha256Hex(sb.toString());
  }

  /**
   * Pre-flight: decide whether idempotency applies to this request and, if so, look up an existing
   * record for {@code (realm, key)}.
   *
   * <p>All of the work lives here: if idempotency is disabled or no key was supplied, this returns
   * {@link IdempotencyOutcome.Disabled} and the handler runs its plain path. Otherwise the single
   * binding hash is computed from the raw inputs and the store is consulted:
   *
   * <ul>
   *   <li>No record → {@link IdempotencyOutcome.New} (carrying the computed binding); the handler
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
    IdempotencyOutcome.New newOutcome =
        new IdempotencyOutcome.New(
            idempotencyKey.get(), operation, bindingHash(operation, principal, resourceComponents));
    return lookup(newOutcome);
  }

  /**
   * Records the terminal outcome of an operation that just completed.
   *
   * <p>Takes the {@code preflight} outcome and decides whether to write: only an {@link
   * IdempotencyOutcome.New} (idempotency in effect, no prior record) is recorded; for {@link
   * IdempotencyOutcome.Disabled} the call is a no-op and the outcome is returned unchanged.
   *
   * <p>The insert is atomic on {@code (realm, key)}; if another caller raced ahead and inserted
   * first, this returns {@link IdempotencyOutcome.Duplicate} carrying that existing record so the
   * handler can rebuild an equivalent response from current state. Otherwise the {@code preflight}
   * outcome is returned to signal a clean win.
   */
  public IdempotencyOutcome recordOutcome(
      IdempotencyOutcome preflight, int httpStatus, @Nullable String metadataLocation) {
    if (!(preflight instanceof IdempotencyOutcome.New newOutcome)) {
      return preflight;
    }
    Instant now = clock.instant();
    Instant expiresAt = now.plus(configuration.ttl());
    IdempotencyStore.RecordResult result =
        store.recordIfAbsent(
            newOutcome.idempotencyKey(),
            newOutcome.operation().wireName(),
            newOutcome.bindingHash(),
            httpStatus,
            metadataLocation,
            now,
            expiresAt);
    if (result.type() == IdempotencyStore.RecordResultType.OWNED) {
      return newOutcome;
    }
    IdempotencyRecord existing =
        result
            .existing()
            .orElseThrow(() -> new IllegalStateException("DUPLICATE result without record"));
    return matchOrConflict(existing, newOutcome);
  }

  /**
   * After a concurrent {@code createTable} loses the catalog race (AlreadyExistsException), checks
   * whether the race winner recorded a matching idempotency outcome so this caller can replay it.
   *
   * <p>Takes the {@code preflight} outcome: only an {@link IdempotencyOutcome.New} can race, so for
   * any other outcome (e.g. {@link IdempotencyOutcome.Disabled}) this returns empty immediately.
   * The winner records its outcome only after committing the table, so a loser may observe the
   * conflict slightly before the record is visible; this polls a bounded number of times. A binding
   * mismatch surfaces as {@link IdempotencyConflictException} (mapped to 422). Returns empty if no
   * matching record appears, meaning the conflict was a genuine pre-existing table rather than a
   * same-key retry.
   */
  public Optional<IdempotencyRecord> resolveConcurrentDuplicate(IdempotencyOutcome preflight) {
    if (!(preflight instanceof IdempotencyOutcome.New newOutcome)) {
      return Optional.empty();
    }
    long backoffMillis = configuration.concurrentReplayInitialBackoff().toMillis();
    int maxAttempts = configuration.concurrentReplayMaxAttempts();
    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      if (lookup(newOutcome) instanceof IdempotencyOutcome.Duplicate dup) {
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

  /**
   * Single store lookup: returns {@code newOutcome} if no record exists, else matches/conflicts.
   */
  private IdempotencyOutcome lookup(IdempotencyOutcome.New newOutcome) {
    return store
        .load(newOutcome.idempotencyKey())
        .map(record -> matchOrConflict(record, newOutcome))
        .orElse(newOutcome);
  }

  private static IdempotencyOutcome matchOrConflict(
      IdempotencyRecord existing, IdempotencyOutcome.New expected) {
    if (!expected.bindingHash().equals(existing.bindingHash())) {
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
    public int concurrentReplayMaxAttempts() {
      return 5;
    }

    @Override
    public java.time.Duration concurrentReplayInitialBackoff() {
      return java.time.Duration.ofMillis(5);
    }
  }
}

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
package org.apache.polaris.realms.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.polaris.realms.api.RealmDefinition.RealmStatus.ACTIVE;
import static org.apache.polaris.realms.api.RealmDefinition.RealmStatus.CREATED;
import static org.apache.polaris.realms.api.RealmDefinition.RealmStatus.INACTIVE;
import static org.apache.polaris.realms.api.RealmDefinition.RealmStatus.INITIALIZING;
import static org.apache.polaris.realms.api.RealmDefinition.RealmStatus.LOADING;
import static org.apache.polaris.realms.api.RealmDefinition.RealmStatus.PURGED;
import static org.apache.polaris.realms.api.RealmDefinition.RealmStatus.PURGING;

import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.realms.api.ImmutableRealmDefinition;
import org.apache.polaris.realms.api.RealmDefinition;
import org.apache.polaris.realms.api.RealmExpectedStateMismatchException;
import org.apache.polaris.realms.api.RealmManagement;
import org.apache.polaris.realms.spi.RealmStore;

@ApplicationScoped
class RealmManagementImpl implements RealmManagement {
  private static final int REALM_ID_MAX_LENGTH = 128;

  private final RealmStore store;
  private final Supplier<Instant> clock;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  RealmManagementImpl(RealmStore store, MonotonicClock clock) {
    this(store, clock::currentInstant);
  }

  RealmManagementImpl(RealmStore store, Supplier<Instant> clock) {
    this.store = store;
    this.clock = clock;
  }

  @Override
  @Nonnull
  public Stream<RealmDefinition> list() {
    return store.list();
  }

  private static void validateRealmId(@Nonnull String realmId) {
    checkArgument(
        !realmId.startsWith("::") && !realmId.isEmpty() && realmId.length() <= REALM_ID_MAX_LENGTH,
        "Invalid realm ID");
  }

  @Override
  @Nonnull
  public Optional<RealmDefinition> get(@Nonnull String realmId) {
    validateRealmId(realmId);

    return store.get(realmId);
  }

  @Override
  @Nonnull
  public RealmDefinition create(@Nonnull String realmId) {
    validateRealmId(realmId);

    var now = clock.get();
    return store.create(
        realmId,
        ImmutableRealmDefinition.builder()
            .status(CREATED)
            .id(realmId)
            .created(now)
            .updated(now)
            .build());
  }

  private void verifyStateTransition(RealmDefinition expected, RealmDefinition update) {
    switch (expected.status()) {
      case CREATED ->
          checkArgument(
              update.status() == CREATED
                  || update.status() == LOADING
                  || update.status() == INITIALIZING
                  || update.status() == PURGING,
              "Invalid realm state transition from %s to %s",
              expected.status(),
              update.status());
      case LOADING ->
          checkArgument(
              update.status() == INACTIVE
                  || update.status() == ACTIVE
                  || update.status() == PURGING,
              "Invalid realm state transition from %s to %s",
              expected.status(),
              update.status());
      case INITIALIZING ->
          checkArgument(
              update.status() == CREATED
                  || update.status() == INACTIVE
                  || update.status() == ACTIVE
                  || update.status() == PURGING,
              "Invalid realm state transition from %s to %s",
              expected.status(),
              update.status());
      case ACTIVE ->
          checkArgument(
              update.status() == ACTIVE || update.status() == INACTIVE,
              "Invalid realm state transition from %s to %s",
              expected.status(),
              update.status());
      case INACTIVE ->
          checkArgument(
              update.status() == ACTIVE
                  || update.status() == INACTIVE
                  || update.status() == PURGING,
              "Invalid realm state transition from %s to %s",
              expected.status(),
              update.status());
      case PURGING ->
          checkArgument(
              update.status() == PURGING || update.status() == PURGED,
              "Invalid realm state transition from %s to %s",
              expected.status(),
              update.status());
      case PURGED ->
          checkArgument(
              update.status() == PURGING,
              "Invalid realm state transition from %s to %s",
              expected.status(),
              update.status());
      default -> throw new IllegalStateException("Unknown realm status " + expected.status());
    }
  }

  @Override
  @Nonnull
  public RealmDefinition update(
      @Nonnull RealmDefinition expected, @Nonnull RealmDefinition update) {
    validateRealmId(expected.id());
    var realmId = expected.id();
    checkArgument(
        realmId.equals(update.id()), "Expected and update must contain the same realm ID");

    verifyStateTransition(expected, update);

    return store.update(
        realmId,
        current -> {
          if (!current.equals(expected)) {
            throw new RealmExpectedStateMismatchException(
                "Realm does not match the expected state");
          }
          var now = clock.get();
          return ImmutableRealmDefinition.builder().from(update).updated(now).build();
        });
  }

  @Override
  public void delete(@Nonnull RealmDefinition expected) {
    var realmId = expected.id();
    validateRealmId(realmId);
    checkArgument(expected.status() == PURGED, "Realm must be in state %s to be deleted", PURGED);

    store.delete(
        realmId,
        current -> {
          if (!current.equals(expected)) {
            throw new RealmExpectedStateMismatchException(
                "Realm does not match the expected state");
          }
        });
  }
}

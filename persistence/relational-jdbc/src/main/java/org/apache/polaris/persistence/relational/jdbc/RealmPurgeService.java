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
package org.apache.polaris.persistence.relational.jdbc;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drives the async, batched purge of realm-scoped time-series data. Realms are enqueued into {@code
 * realm_purge} on purge; this service later drains them in bounded batches. Per-store deletion is
 * delegated to pluggable {@link RealmDataPurger}s.
 */
public class RealmPurgeService {

  private static final Logger LOGGER = LoggerFactory.getLogger(RealmPurgeService.class);

  private final RealmPurgeStore store;
  private final List<RealmDataPurger> purgers;

  public RealmPurgeService(RealmPurgeStore store, List<RealmDataPurger> purgers) {
    this.store = store;
    this.purgers = List.copyOf(purgers);
  }

  /** Records that {@code realmId}'s time-series data should be purged asynchronously. */
  public void enqueue(String realmId, long nowMs) {
    store.enqueue(realmId, nowMs);
  }

  /**
   * Drains all currently claimable realms, deleting each realm's data in {@code batchSize} batches
   * until empty, then removing its marker. A realm owned by another node is skipped; a realm whose
   * drain is interrupted resumes on a later run once its claim lease expires.
   *
   * @param node identifier of the draining node, recorded on the claim
   * @param batchSize maximum rows deleted per statement
   * @param claimLeaseMs how long a claim is honored before another node may steal it
   * @param nowMs current wall-clock time in millis
   * @return the number of realms fully drained during this run
   */
  public int drainOnce(String node, int batchSize, long claimLeaseMs, long nowMs) {
    // A non-positive batch would delete nothing yet still clear the marker, discarding data.
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be strictly positive, got " + batchSize);
    }
    long staleClaimBeforeMs = nowMs - claimLeaseMs;
    int drained = 0;
    for (String realmId : store.listClaimable(staleClaimBeforeMs)) {
      if (!store.claim(realmId, node, nowMs, staleClaimBeforeMs)) {
        continue; // another node owns this realm
      }
      // Isolate each realm: a failure must not abort the run. The failed realm keeps its marker
      // and is retried once its claim lease expires.
      try {
        drainRealm(realmId, batchSize);
        store.complete(realmId);
        drained++;
      } catch (RuntimeException e) {
        LOGGER.warn(
            "Failed to drain realm {} for purge; leaving it queued for retry on a later run",
            realmId,
            e);
      }
    }
    return drained;
  }

  private void drainRealm(String realmId, int batchSize) {
    boolean anyRemaining = true;
    while (anyRemaining) {
      anyRemaining = false;
      for (RealmDataPurger purger : purgers) {
        if (purger.purgeBatch(realmId, batchSize) > 0) {
          anyRemaining = true;
        }
      }
    }
  }
}

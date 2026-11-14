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

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.polaris.persistence.relational.jdbc.RealmPurgeStore.PendingPurge;
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
  private final Clock clock;

  public RealmPurgeService(RealmPurgeStore store, List<RealmDataPurger> purgers, Clock clock) {
    this.store = store;
    this.purgers = List.copyOf(purgers);
    this.clock = clock;
  }

  /**
   * Drains claimable realms, deleting each realm's data in {@code batchSize} batches until empty,
   * then removing its marker. A realm owned by another node is skipped; a realm whose drain is
   * interrupted resumes on a later run once its claim lease expires.
   *
   * @param node identifier of the draining node, recorded on the claim
   * @param batchSize maximum rows deleted per statement
   * @param claimLeaseMs how long a claim is honored before another node may take it over
   * @param realms realms to drain, or empty to drain every queued realm
   * @param progress receives a line per realm as the drain proceeds, for callers that report to a
   *     console rather than only to the log
   * @return the number of realms fully drained during this run
   */
  public int drainOnce(
      String node,
      int batchSize,
      long claimLeaseMs,
      Collection<String> realms,
      Consumer<String> progress) {
    // A non-positive batch would delete nothing yet still clear the marker, discarding data.
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be strictly positive, got " + batchSize);
    }
    if (claimLeaseMs <= 0) {
      throw new IllegalArgumentException(
          "claimLeaseMs must be strictly positive, got " + claimLeaseMs);
    }

    List<PendingPurge> claimable = store.listClaimable(clock.millis() - claimLeaseMs);
    Set<String> requested = new LinkedHashSet<>(realms);
    if (!requested.isEmpty()) {
      claimable = claimable.stream().filter(p -> requested.contains(p.realmId())).toList();
      claimable.forEach(p -> requested.remove(p.realmId()));
      // A realm left out of the claimable set either has nothing queued or is being drained
      // elsewhere; those read the same in a count of zero, so say which.
      requested.forEach(r ->
          progress.accept(
              store
                  .claimedBy(r)
                  .map(owner -> "Realm " + r + " is being drained by " + owner + ".")
                  .orElse("Realm " + r + " has no queued purge.")));
    }

    int drained = 0;
    for (PendingPurge pending : claimable) {
      String realmId = pending.realmId();
      // Each claim is timed when it is taken, so a realm drained late in a long run is not given a
      // lease that already expired before its first batch.
      long claimedAtMs = clock.millis();
      if (!store.claim(realmId, node, claimedAtMs, claimedAtMs - claimLeaseMs)) {
        progress.accept(
            "Skipped realm " + realmId + ", drained by " + owner(realmId) + " right now.");
        continue;
      }
      // Isolate each realm: a failure must not abort the run. The failed realm keeps its marker
      // and is retried once its claim lease expires.
      try {
        Outcome outcome = drainRealm(pending, node, batchSize, progress);
        if (outcome.completed() && store.complete(realmId, node)) {
          drained++;
          progress.accept("Drained realm " + realmId + ", deleted " + outcome.rows() + " row(s).");
        }
      } catch (RuntimeException e) {
        LOGGER.warn(
            "Failed to drain realm {} for purge; leaving it queued for retry on a later run",
            realmId,
            e);
        progress.accept(
            "Failed to drain realm "
                + realmId
                + ", left queued for a later run: "
                + e.getMessage());
      }
    }
    return drained;
  }

  /**
   * Deletes the realm's data a batch at a time, renewing the claim before each round so that a
   * drain outliving its lease stops instead of racing the node that took it over.
   */
  private Outcome drainRealm(
      PendingPurge pending, String node, int batchSize, Consumer<String> progress) {
    String realmId = pending.realmId();
    long rows = 0;
    // A store that returns fewer rows than the batch had no more to give, and the cutoff is fixed,
    // so it cannot acquire any later in this drain. Dropping it keeps the remaining rounds from
    // re-querying a store that is already empty, which costs a scan per round where the table has
    // no index for the realm.
    List<RealmDataPurger> remaining = new ArrayList<>(purgers);
    while (!remaining.isEmpty()) {
      if (!store.renew(realmId, node, clock.millis())) {
        String owner = owner(realmId);
        LOGGER.info(
            "Stopped draining realm {} for purge: the claim is now held by {}", realmId, owner);
        progress.accept(
            "Stopped draining realm "
                + realmId
                + " after "
                + rows
                + " row(s), the claim is now held by "
                + owner
                + ".");
        return new Outcome(false, rows);
      }
      for (Iterator<RealmDataPurger> purger = remaining.iterator(); purger.hasNext(); ) {
        long deleted = purger.next().purgeBatch(realmId, pending.requestedAtMs(), batchSize);
        rows += deleted;
        if (deleted < batchSize) {
          purger.remove();
        }
      }
    }
    return new Outcome(true, rows);
  }

  private String owner(String realmId) {
    return store.claimedBy(realmId).orElse("another drainer");
  }

  /** What a realm's drain achieved: whether it finished, and how many rows it deleted. */
  private record Outcome(boolean completed, long rows) {}
}

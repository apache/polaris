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
package org.apache.polaris.persistence.nosql.maintenance.impl;

import static org.apache.polaris.persistence.nosql.api.Realms.SYSTEM_REALM_PREFIX;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;

import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.backend.PersistId;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ScanHandler<I> implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScanHandler.class);

  final String name;
  final RateLimit rateLimit;
  final long maxCreatedAtMicros;
  final Set<String> realmsToRetain;
  final Set<String> realmsToPurge;
  final Consumer<String> seenRealmsToPurge;
  final RetainCheck<I> retainCheck;
  final int deleteBatchSize;
  final Consumer<Map<String, Set<I>>> batchDelete;
  final ScanItemCallback<I> itemCallback;

  final Map<String, Set<I>> deletions = new HashMap<>();
  int numDeletes;

  ScanHandler(
      String name,
      OptionalInt rateLimit,
      long maxCreatedAtMicros,
      Set<String> realmsToRetain,
      Set<String> realmsToPurge,
      Consumer<String> seenRealmsToPurge,
      RetainCheck<I> retainCheck,
      int deleteBatchSize,
      Consumer<Map<String, Set<I>>> batchDelete,
      ScanItemCallback<I> itemCallback) {
    this.name = name;
    this.rateLimit = RateLimit.create(rateLimit.orElse(-1));
    this.maxCreatedAtMicros = maxCreatedAtMicros;
    this.realmsToRetain = realmsToRetain;
    this.realmsToPurge = realmsToPurge;
    this.seenRealmsToPurge = seenRealmsToPurge;
    this.retainCheck = retainCheck;
    this.deleteBatchSize = deleteBatchSize;
    this.batchDelete = batchDelete;
    this.itemCallback = itemCallback;
  }

  void scanned(String realmId, I id, long createdAtMicros) {
    if (realmId.startsWith(SYSTEM_REALM_PREFIX) && !realmsToRetain.contains(realmId)) {
      // some system realm, ignore
      return;
    }

    rateLimit.acquire();
    ScanItemOutcome outcome;
    if (realmsToPurge.contains(realmId)) {
      outcome = ScanItemOutcome.REALM_PURGE;
      purge(realmId, id);
      seenRealmsToPurge.accept(realmId);
    } else if (createdAtMicros > maxCreatedAtMicros) {
      outcome = ScanItemOutcome.TOO_NEW_RETAINED;
    } else if (realmsToRetain.contains(realmId)) {
      if (retainCheck.check(realmId, id)) {
        outcome = ScanItemOutcome.RETAINED;
      } else {
        outcome = ScanItemOutcome.PURGED;
        purge(realmId, id);
      }
    } else {
      outcome = ScanItemOutcome.UNHANDLED_RETAINED;
    }
    itemCallback.itemOutcome(realmId, id, outcome);
    LOGGER.debug(
        "Got '{}' {} {} -> {}, createdAtMicros = {}",
        realmId,
        name,
        id,
        outcome.message,
        createdAtMicros);
  }

  private void purge(String realmId, I id) {
    LOGGER.debug("Enqueuing delete for '{}' {}", realmId, id);
    deletions.computeIfAbsent(realmId, k -> new HashSet<>()).add(id);
    numDeletes++;
    if (numDeletes == deleteBatchSize) {
      flushDeletes();
    }
  }

  private void flushDeletes() {
    LOGGER.debug("Flushing {} {} deletions", numDeletes, name);
    batchDelete.accept(deletions);
    deletions.clear();
    numDeletes = 0;
  }

  @Override
  public void close() {
    if (numDeletes > 0) {
      flushDeletes();
    }
  }

  public Backend.ObjScanCallback asObjScanCallback(LongSupplier clock) {
    return new ProgressObjScanCallback(clock);
  }

  public Backend.ReferenceScanCallback asReferenceScanCallback(LongSupplier clock) {
    return new ProgressReferenceScanCallback(clock);
  }

  @FunctionalInterface
  interface RetainCheck<I> {
    boolean check(String realm, I id);
  }

  private abstract static class ProgressCallback {
    private final LongSupplier clock;
    private long nextLog;
    private long scanned;

    ProgressCallback(LongSupplier clock) {
      this.clock = clock;
      nextLog = clock.getAsLong() + 2_000L;
    }

    protected void called(String what) {
      var s = scanned++;
      var now = clock.getAsLong();
      if (now >= nextLog) {
        LOGGER.info("... scanned {} {} so far", s, what);
        nextLog = now + 2_000L;
      }
    }
  }

  private class ProgressReferenceScanCallback extends ProgressCallback
      implements Backend.ReferenceScanCallback {

    ProgressReferenceScanCallback(LongSupplier clock) {
      super(clock);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void call(@Nonnull String realmId, @Nonnull String refName, long createdAtMicros) {
      called("references");
      ((ScanHandler<String>) ScanHandler.this).scanned(realmId, refName, createdAtMicros);
    }
  }

  private class ProgressObjScanCallback extends ProgressCallback
      implements Backend.ObjScanCallback {
    ProgressObjScanCallback(LongSupplier clock) {
      super(clock);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void call(
        @Nonnull String realmId,
        @Nonnull String type,
        @Nonnull PersistId id,
        long createdAtMicros) {
      called("objects");
      ((ScanHandler<ObjRef>) ScanHandler.this)
          .scanned(realmId, objRef(type, id.id(), id.part()), createdAtMicros);
    }
  }
}

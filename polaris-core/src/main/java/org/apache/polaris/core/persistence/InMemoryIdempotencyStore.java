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
package org.apache.polaris.core.persistence;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.core.entity.IdempotencyRecord;

/**
 * Simple in-memory {@link IdempotencyStore} implementation.
 *
 * <p>Intended for dev/test and in-memory Polaris deployments; not durable across restarts.
 */
public final class InMemoryIdempotencyStore implements IdempotencyStore {

  private static final int PURGE_EVERY_N_RESERVES = 256;

  private static final class Key {
    private final String realmId;
    private final String idempotencyKey;

    private Key(String realmId, String idempotencyKey) {
      this.realmId = realmId;
      this.idempotencyKey = idempotencyKey;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Key other)) {
        return false;
      }
      return realmId.equals(other.realmId) && idempotencyKey.equals(other.idempotencyKey);
    }

    @Override
    public int hashCode() {
      int result = realmId.hashCode();
      result = 31 * result + idempotencyKey.hashCode();
      return result;
    }
  }

  private final ConcurrentMap<Key, RecordState> records = new ConcurrentHashMap<>();
  private final AtomicInteger reserveCount = new AtomicInteger();

  private static final class RecordState {
    volatile IdempotencyRecord record;

    RecordState(IdempotencyRecord record) {
      this.record = record;
    }
  }

  @Override
  public ReserveResult reserve(
      String realmId,
      String idempotencyKey,
      String operationType,
      String normalizedResourceId,
      Instant expiresAt,
      String executorId,
      Instant now) {
    maybePurgeExpired(realmId, now);

    Key key = new Key(realmId, idempotencyKey);
    IdempotencyRecord initial =
        new IdempotencyRecord(
            realmId,
            idempotencyKey,
            operationType,
            normalizedResourceId,
            null,
            null,
            null,
            null,
            now,
            now,
            null,
            now,
            executorId,
            expiresAt);

    // Atomic check-and-reserve: expired records are replaced in the same lock scope.
    RecordState[] duplicate = new RecordState[1];
    records.compute(
        key,
        (k, existing) -> {
          if (existing != null) {
            Instant exp = existing.record.expiresAt();
            if (exp == null || !exp.isBefore(now)) {
              duplicate[0] = existing;
              return existing;
            }
          }
          return new RecordState(initial);
        });

    if (duplicate[0] != null) {
      return new ReserveResult(ReserveResultType.DUPLICATE, Optional.of(duplicate[0].record));
    }
    return new ReserveResult(ReserveResultType.OWNED, Optional.empty());
  }

  @Override
  public Optional<IdempotencyRecord> load(String realmId, String idempotencyKey) {
    Key key = new Key(realmId, idempotencyKey);
    IdempotencyRecord[] result = new IdempotencyRecord[1];
    records.computeIfPresent(
        key,
        (k, existing) -> {
          Instant exp = existing.record.expiresAt();
          if (exp != null && exp.isBefore(Instant.now())) {
            return null;
          }
          result[0] = existing.record;
          return existing;
        });
    return Optional.ofNullable(result[0]);
  }

  @Override
  public HeartbeatResult updateHeartbeat(
      String realmId, String idempotencyKey, String executorId, Instant now) {
    Key key = new Key(realmId, idempotencyKey);
    RecordState state = records.get(key);
    if (state == null) {
      return HeartbeatResult.NOT_FOUND;
    }

    synchronized (state) {
      IdempotencyRecord record = state.record;
      Instant expiresAt = record.expiresAt();
      if (expiresAt != null && expiresAt.isBefore(now)) {
        records.remove(key, state);
        return HeartbeatResult.NOT_FOUND;
      }
      if (record.httpStatus() != null) {
        return HeartbeatResult.FINALIZED;
      }
      if (record.executorId() == null || !record.executorId().equals(executorId)) {
        return HeartbeatResult.LOST_OWNERSHIP;
      }

      state.record =
          new IdempotencyRecord(
              record.realmId(),
              record.idempotencyKey(),
              record.operationType(),
              record.normalizedResourceId(),
              record.httpStatus(),
              record.errorSubtype(),
              record.responseSummary(),
              record.responseHeaders(),
              record.createdAt(),
              now,
              record.finalizedAt(),
              now,
              record.executorId(),
              record.expiresAt());
      return HeartbeatResult.UPDATED;
    }
  }

  @Override
  public boolean cancelInProgressReservation(
      String realmId, String idempotencyKey, String executorId) {
    Key key = new Key(realmId, idempotencyKey);
    RecordState state = records.get(key);
    if (state == null) {
      return false;
    }
    synchronized (state) {
      IdempotencyRecord record = state.record;
      Instant expiresAt = record.expiresAt();
      if (expiresAt != null && expiresAt.isBefore(Instant.now())) {
        records.remove(key, state);
        return false;
      }
      if (record.httpStatus() != null) {
        return false;
      }
      if (record.executorId() == null || !record.executorId().equals(executorId)) {
        return false;
      }
      return records.remove(key, state);
    }
  }

  @Override
  public boolean finalizeRecord(
      String realmId,
      String idempotencyKey,
      String executorId,
      Integer httpStatus,
      String errorSubtype,
      String responseSummary,
      Map<String, String> responseHeaders,
      Instant finalizedAt) {
    Key key = new Key(realmId, idempotencyKey);
    RecordState state = records.get(key);
    if (state == null) {
      return false;
    }

    synchronized (state) {
      IdempotencyRecord record = state.record;
      Instant expiresAt = record.expiresAt();
      if (expiresAt != null && expiresAt.isBefore(finalizedAt)) {
        records.remove(key, state);
        return false;
      }
      if (record.httpStatus() != null) {
        return false;
      }
      if (record.executorId() == null || !record.executorId().equals(executorId)) {
        return false;
      }

      state.record =
          new IdempotencyRecord(
              record.realmId(),
              record.idempotencyKey(),
              record.operationType(),
              record.normalizedResourceId(),
              httpStatus,
              errorSubtype,
              responseSummary,
              responseHeaders,
              record.createdAt(),
              finalizedAt,
              finalizedAt,
              record.heartbeatAt(),
              record.executorId(),
              record.expiresAt());
      return true;
    }
  }

  @Override
  public int purgeExpired(String realmId, Instant before) {
    int[] count = {0};
    records.forEach(
        (k, v) -> {
          Instant expiresAt = v.record.expiresAt();
          if (k.realmId.equals(realmId) && expiresAt != null && expiresAt.isBefore(before)) {
            if (records.remove(k, v)) {
              count[0]++;
            }
          }
        });
    return count[0];
  }

  private void maybePurgeExpired(String realmId, Instant now) {
    // PURGE_EVERY_N_RESERVES is a power of two; use bitmask instead of modulo.
    if ((reserveCount.incrementAndGet() & (PURGE_EVERY_N_RESERVES - 1)) != 0) {
      return;
    }
    purgeExpired(realmId, now);
  }
}

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
import org.apache.polaris.core.entity.IdempotencyRecord;

/**
 * Simple in-memory {@link IdempotencyStore} implementation.
 *
 * <p>Intended for dev/test and in-memory Polaris deployments; not durable across restarts.
 */
public final class InMemoryIdempotencyStore implements IdempotencyStore {

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

    RecordState existing = records.putIfAbsent(key, new RecordState(initial));
    if (existing == null) {
      return new ReserveResult(ReserveResultType.OWNED, Optional.empty());
    }
    return new ReserveResult(ReserveResultType.DUPLICATE, Optional.of(existing.record));
  }

  @Override
  public Optional<IdempotencyRecord> load(String realmId, String idempotencyKey) {
    RecordState s = records.get(new Key(realmId, idempotencyKey));
    return s == null ? Optional.empty() : Optional.of(s.record);
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
          if (k.realmId.equals(realmId) && v.record.expiresAt().isBefore(before)) {
            if (records.remove(k, v)) {
              count[0]++;
            }
          }
        });
    return count[0];
  }
}

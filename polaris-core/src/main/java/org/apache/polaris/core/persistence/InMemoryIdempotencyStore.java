/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.core.persistence;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.polaris.core.entity.IdempotencyRecord;

/**
 * In-memory {@link IdempotencyStore} backed by a {@link ConcurrentHashMap}.
 *
 * <p>Suitable for dev/test and the in-memory Polaris deployment. Not durable across restarts.
 */
public final class InMemoryIdempotencyStore implements IdempotencyStore {

  private record Key(String realmId, String idempotencyKey) {}

  private final ConcurrentMap<Key, IdempotencyRecord> records = new ConcurrentHashMap<>();

  @Override
  public Optional<IdempotencyRecord> load(String realmId, String idempotencyKey) {
    return Optional.ofNullable(records.get(new Key(realmId, idempotencyKey)));
  }

  @Override
  public RecordResult recordIfAbsent(
      String realmId,
      String idempotencyKey,
      String operationType,
      String resourceHash,
      String principalHash,
      int httpStatus,
      String metadataLocation,
      Instant createdAt,
      Instant expiresAt) {
    Key key = new Key(realmId, idempotencyKey);
    IdempotencyRecord candidate =
        new IdempotencyRecord(
            realmId,
            idempotencyKey,
            operationType,
            resourceHash,
            principalHash,
            httpStatus,
            metadataLocation,
            createdAt,
            expiresAt);
    IdempotencyRecord existing = records.putIfAbsent(key, candidate);
    if (existing == null) {
      return new RecordResult(RecordResultType.OWNED, Optional.empty());
    }
    return new RecordResult(RecordResultType.DUPLICATE, Optional.of(existing));
  }

  @Override
  public int purgeExpired(String realmId, Instant before) {
    int purged = 0;
    for (Iterator<Map.Entry<Key, IdempotencyRecord>> it = records.entrySet().iterator();
        it.hasNext(); ) {
      Map.Entry<Key, IdempotencyRecord> entry = it.next();
      if (!entry.getKey().realmId().equals(realmId)) {
        continue;
      }
      Instant expires = entry.getValue().expiresAt();
      if (expires != null && expires.isBefore(before)) {
        it.remove();
        purged++;
      }
    }
    return purged;
  }
}

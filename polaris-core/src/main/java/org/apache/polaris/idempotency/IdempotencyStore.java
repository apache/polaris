/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.polaris.idempotency;

import java.time.Instant;
import java.util.Optional;

public interface IdempotencyStore {
  enum ReserveResultType { OWNED, DUPLICATE }

  final class ReserveResult {
    private final ReserveResultType type;
    private final Optional<IdempotencyRecord> existing;

    public ReserveResult(ReserveResultType type, Optional<IdempotencyRecord> existing) {
      this.type = type;
      this.existing = existing == null ? Optional.empty() : existing;
    }

    public ReserveResultType getType() { return type; }
    public Optional<IdempotencyRecord> getExisting() { return existing; }
  }

  ReserveResult reserve(
      String realmId,
      String idempotencyKey,
      String operationType,
      String normalizedResourceId,
      Instant expiresAt,
      String executorId,
      Instant now);

  Optional<IdempotencyRecord> load(String realmId, String idempotencyKey);

  boolean updateHeartbeat(String realmId, String idempotencyKey, String executorId, Instant now);

  boolean finalizeRecord(
      String realmId,
      String idempotencyKey,
      Integer httpStatus,
      String errorSubtype,
      String responseSummary,
      String responseHeaders,
      Instant finalizedAt);

  int purgeExpired(Instant before);
}

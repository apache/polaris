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
package org.apache.polaris.core.entity;

import java.time.Instant;
import java.util.UUID;

/**
 * Immutable snapshot of a recorded idempotency outcome.
 *
 * <p>Under the single-transaction ("optimistic commit") model used by Polaris, a record only exists
 * after the originating operation has succeeded (a terminal 2xx status). There is no separate
 * in-progress state, no executor lease, and no stored response body — replays re-derive an
 * equivalent response from authoritative catalog state. Failed outcomes are not recorded, so a
 * retry after a failure simply re-runs the operation.
 *
 * <p>The {@code (realm_id, idempotency_key)} pair is the logical primary key; {@code binding_hash}
 * is the single value callers compare against on replay to detect reuse of the same key for a
 * different caller, operation, or resource. {@code operation_type} is retained separately purely as
 * a human-readable label for debugging/observability and is not part of the comparison.
 *
 * @param realmId Logical tenant / realm identifier.
 * @param idempotencyKey Client-provided idempotency key (a UUIDv7).
 * @param operationType Logical operation type (e.g. {@code "create-table"}), kept human-readable
 *     for debugging only.
 * @param bindingHash Opaque hash over the full binding (caller principal, operation, and the
 *     request-derived resource identity such as namespace, name and access-delegation modes). Not a
 *     human-readable identifier, and intentionally does not include the request payload; compared
 *     on replay to detect reuse of the same key for a different caller/operation/resource.
 * @param httpStatus HTTP status code that was returned to the client for the originating request.
 * @param metadataLocation Resource state pointer captured when the record was written (for tables,
 *     the metadata-file location). Used on replay to detect that the resource has advanced beyond
 *     the originally-created state; {@code null} for operations that do not track one.
 * @param createdAt Timestamp when the record was inserted.
 * @param expiresAt Timestamp after which the record is eligible for purging.
 */
public record IdempotencyRecord(
    String realmId,
    UUID idempotencyKey,
    String operationType,
    String bindingHash,
    int httpStatus,
    String metadataLocation,
    Instant createdAt,
    Instant expiresAt) {}

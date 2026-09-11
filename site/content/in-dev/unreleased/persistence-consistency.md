---
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
title: Persistence consistency contract
type: docs
weight: 450
---

This page documents the consistency contract between Polaris persistence layers
and the code that calls them. It is intended for Polaris developers and
contributors who implement or use `PolarisMetaStoreManager` and `BasePersistence`.

## Background

Polaris has two closely-related persistence abstractions:

* `PolarisMetaStoreManager` — the high-level manager interface used by the
  REST catalog service and the management service.
* `BasePersistence` — the lower-level SPI implemented by each backend
  (in-memory `TreeMap`, JDBC, NoSQL, etc.).

`BasePersistence` documents that each of its methods represents one atomic
change. `PolarisMetaStoreManager` does not make an equivalent guarantee, which
leaves callers unsure whether the state they read for validation,
authorization, or credential vending is the same state that eventually commits.
This gap is the root cause of the consistency symptoms seen in JDBC-backed
deployments: a manager-level operation can perform several `BasePersistence`
calls, and under JDBC each call may be committed in a separate database
transaction.

The fix is not to add operation-specific multi-object methods to the SPI, nor
to wrap an entire REST request in a single database transaction. Instead,
Polaris introduces a backend-agnostic **change set** primitive that lets a
manager-level operation express an ordered list of related mutations. The
backend reports a per-change-set outcome: either the whole list is applied
atomically, or the backend cannot apply that list atomically and applies
nothing.

Atomicity depends on the storage placement resolved for a specific change set.
The contract does not assume that entity records, grant records, or a later
record family are co-located behind one transactional backend.

## Consistency contract

### What callers can assume

A `PolarisMetaStoreManager` implementation may execute a single manager-level
operation as a logical change set. When it calls `commitTransactionBatch`:

* The backend either commits every mutation in the change set together, or
  reports that it cannot do so for this change set.
* A successful atomic commit is all-or-nothing. If a later mutation fails, earlier
  mutations in the same change set are not visible.
* The backend validates the change set against the state that existed when the
  change set was built. In particular, CAS updates succeed only if the entity
  still matches the original entity the caller supplied.
* If the backend returns `UNSUPPORTED` for this change set, no mutation was
  applied. Callers that want best-effort sequencing use
  `applyChangeSetBestEffort`, which is a separate contract.

Callers must not assume that unrelated change sets are ordered or atomic with
respect to each other. For example, changes in catalog A and catalog B do not
have to be globally atomic.

### What the persistence SPI guarantees

`BasePersistence` continues to guarantee that each single-method call is
atomic where its Javadoc says so. In addition, `commitChangeSet` takes one
ordered list of `PersistenceMutation` items and returns a per-change-set
outcome:

* `APPLIED` — every mutation was committed in one atomic step.
* `UNSUPPORTED` — this backend cannot atomically apply this change set. No
  mutation was applied.

`commitChangeSet` does not fall back to individual writes. A composite backend
must not claim cross-store atomicity; it returns `UNSUPPORTED` for a change set
whose records are not co-located.

Conflicts are reported by throwing `EntityAlreadyExistsException` or
`RetryOnConcurrencyException`. The manager maps those to the existing
`EntitiesResult` statuses (`ENTITY_ALREADY_EXISTS` and
`TARGET_ENTITY_CONCURRENTLY_MODIFIED`). `CHANGE_SET_ATOMICITY_UNSUPPORTED`
means the backend declined the change set without applying anything.

### What a change set is not

A logical change set is **not** the same thing as a request-scope database
transaction. The JDBC backend may implement a change set as a short database
transaction, but that is an implementation detail. The change set must not span
slow external work such as object-storage writes, credential vending, or calls
to external authorizers.

## Change-set primitive

### Persistence mutations

`PersistenceMutation` is one item in the ordered SPI list. Each item carries:

* `kind` — the durable record family (`ENTITY`, `GRANT_RECORD`; later families
  such as policy mappings or secrets add a kind without widening
  `BasePersistence`).
* `operation` — `CREATE`, `UPDATE`, or `DELETE`.
* target, payload, and preconditions — supplied by the kind-specific record:
  * `PersistenceMutation.Entity` — target is the entity identity, payload is
    `entity`, preconditions are `originalEntity` (required for `UPDATE`).
  * `PersistenceMutation.Grant` — target and payload are the grant-record
    primary key; there are no preconditions. Duplicate creates are a no-op.

### Manager-level change set

`MetaStoreChangeSet` groups entity creates, CAS updates, and grant-record
creates/deletes and flattens them to an ordered `PersistenceMutation` list via
`toMutations()`. It is the unit of work passed to:

* `commitTransactionBatch` — atomic, or `CHANGE_SET_ATOMICITY_UNSUPPORTED`.
* `applyChangeSetBestEffort` — sequences existing per-record manager
  operations. Creates and updates are separate writes so they honor the
  `writeEntities` CREATE-xor-UPDATE constraint. Phase 1 does not apply grant
  mutations on this path, because backends such as NoSQL store grants outside
  `BasePersistence#writeToGrantRecords`.

### Backend-level change set

`BasePersistence#commitChangeSet(List<PersistenceMutation>)` is the backend
hook. A backend that can apply the list atomically does so and returns
`APPLIED`. It reports conflicts by throwing a concurrency exception, not by
partially applying the change set.

Transactional backends expose `commitChangeSetInCurrentTxn` so the manager can
own the transaction boundary. `commitChangeSet` opens a transaction and
delegates to that primitive; it must not be called from inside an already-open
transaction.

### Optimistic concurrency (CAS)

`PersistenceMutation.Entity` `UPDATE` uses `originalEntity` as the CAS
baseline. The backend commit succeeds only if the persisted entity still
matches the baseline. If the entity has changed concurrently, the backend
reports a conflict and the manager operation decides whether to retry, fail, or
reconcile.

This makes the change set suitable for operations such as rename validation
and grant-record version bumps, where the caller must ensure that the entities
it read have not been modified by another request.

## Retry semantics

Retry belongs at the manager level, not inside an individual backend commit.
The backend performs one atomic attempt and returns a precise outcome. If the
outcome is a retriable conflict (optimistic-lock failure, transaction
serialization error, transient timeout), the manager may rebuild the change set
from fresh state and attempt the commit again.

Because a change set is short and does not span external systems, retrying it
does not risk duplicating object-storage writes, STS credential issuance, or
other external effects. Those external phases are separate from the persistence
commit phase and must be coordinated by the caller with their own idempotency
or reconciliation logic.

## Implementation phases

The work is expected to land in roughly this order:

1. **SPI foundation** — add CAS-aware `PersistenceMutation` and
   `BasePersistence#commitChangeSet` with a per-change-set `APPLIED` or
   `UNSUPPORTED` outcome; implement the method in JDBC and TreeMap; add the
   corresponding manager-level `MetaStoreChangeSet`, `commitTransactionBatch`,
   and `applyChangeSetBestEffort` wiring. This phase does not implement
   routing or cross-store coordination.
2. **Operation migration** — migrate `createCatalog`, `dropEntity`,
   `renameEntity`, and grant/revoke paths to build a single change set per
   logical operation. Each migrated operation becomes atomic on backends that
   return `APPLIED` and uses existing per-record APIs when the backend returns
   `UNSUPPORTED`.
3. **Authorization integration** — ensure that RBAC authorization checks that
   must be consistent with the persistence commit are performed inside the
   change-set window, and document how external authorizers (OPA, Ranger) fit
   into the contract.

## Relationship to other work

This contract is the foundation for several in-flight improvements:

* Atomic catalog creation with its admin role and grants.
* Atomic entity drop with cleanup tasks.
* Consistent rename validation.
* Authorization-based filtering of list operations.
* Credential-vending decisions that reflect a stable catalog state.

By defining the contract once at the manager level, individual features can
rely on it instead of introducing new operation-specific SPI methods or
ad-hoc retry loops.

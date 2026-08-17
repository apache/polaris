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
manager-level operation express a group of related entity and grant-record
mutations, and asks the backend to commit the whole group atomically when it
can.

## Consistency contract

### What callers can assume

A `PolarisMetaStoreManager` implementation may execute a single manager-level
operation as a logical change set. When it does so:

* All entity and grant-record mutations in the change set are committed
  together, or none of them are.
* The backend validates the change set against the state that existed when the
  change set was built. In particular, CAS updates succeed only if the entity
  still matches the original entity the caller supplied.
* If the backend cannot commit the change set atomically, it must fall back to
  individual `BasePersistence` operations and preserve the existing documented
  atomicity of each individual call.

Callers must not assume that unrelated change sets are ordered or atomic with
respect to each other. For example, changes in catalog A and catalog B do not
have to be globally atomic.

### What the persistence SPI guarantees

`BasePersistence` continues to guarantee that each single-method call is
atomic where its Javadoc says so. In addition, backends that implement the new
`commitChangeSet` method guarantee that the whole list of entity and
grant-record mutations passed to it is applied atomically.

Backends that do not implement `commitChangeSet` throw from the default method.
The manager layer is responsible for detecting this and falling back to
individual `BasePersistence` operations.

### What a change set is not

A logical change set is **not** the same thing as a request-scope database
transaction. The JDBC backend may implement a change set as a short database
transaction, but that is an implementation detail. The change set must not span
slow external work such as object-storage writes, credential vending, or calls
to external authorizers.

## Change-set primitive

### Entity and grant-record mutations

An `EntityMutation` describes one entity change and carries:

* `entity` — the entity to create, update, or delete.
* `originalEntity` — the state the caller read before modifying the entity.
  Required for `UPDATE`; `null` for `CREATE` and `DELETE`.
* `type` — `CREATE`, `UPDATE`, or `DELETE`.

A `GrantMutation` describes one grant-record change and carries:

* `grantRecord` — the grant record to create or delete.
* `type` — `CREATE` or `DELETE`.

### Manager-level change set

`MetaStoreChangeSet` groups a set of `EntityMutation` and `GrantMutation`
records. It is the unit of work passed to the manager-level
`commitTransactionBatch` method.

### Backend-level change set

`BasePersistence#commitChangeSet(List<EntityMutation>, List<GrantMutation>)`
is the backend hook. A backend that supports it applies all mutations in one
atomic step. It reports conflicts by throwing a concurrency exception, not by
partially applying the change set.

### Optimistic concurrency (CAS)

`EntityMutation.UPDATE` uses the `originalEntity` as the CAS baseline. The
backend commit succeeds only if the persisted entity still matches the
baseline. If the entity has changed concurrently, the backend reports a
conflict and the manager operation decides whether to retry, fail, or reconcile.

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

1. **SPI foundation** — add CAS-aware `EntityMutation`, `GrantMutation`, and
   `BasePersistence#commitChangeSet`; implement the method in all non-test
   backends; add the corresponding manager-level `MetaStoreChangeSet` and
   `commitTransactionBatch` wiring. This phase does not refactor existing
   manager operations.
2. **Operation migration** — migrate `createCatalog`, `dropEntity`,
   `renameEntity`, and grant/revoke paths to build a single change set per
   logical operation. Each migrated operation becomes atomic on backends that
   support `commitChangeSet` and degrades gracefully to the existing
   single-call behavior on backends that do not.
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

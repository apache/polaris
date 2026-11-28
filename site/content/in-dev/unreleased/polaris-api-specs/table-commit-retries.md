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
title: 'Did my Iceberg table commit succeed?'
linkTitle: 'Table commit retries'
type: docs
weight: 210
---

If your request times out or Polaris reports an unknown commit state, **keep the files you wrote**.
The commit may have succeeded, and the table may already reference those files.

## Which operations can Polaris replay?

When idempotency is enabled on a native Polaris catalog, support depends on the operation.

| Resource | Operations with key replay | Operations without key replay |
| --- | --- | --- |
| Table | Create a table directly, update an existing table | Finish a staged create, register, drop, or rename a table |
| View | None | Create, register, replace, drop, or rename a view |
| Namespace | None | Create or drop a namespace, update its properties |
| Transaction | None | Commit changes to multiple tables |

For example, the Iceberg 1.12.0 Java client sends a key when registering a table or replacing a
view if the catalog advertises support. Polaris does not replay either operation.

## Before you send the first request

Check whether your native Polaris catalog supports idempotency keys. For example, call
`GET /api/catalog/v1/config?warehouse=analytics` for the `analytics` catalog. If the response
includes `idempotency-key-lifetime`, generate a new key for each supported operation and send it in the
`Idempotency-Key` header. The Iceberg REST specification calls for UUIDv7. Polaris also accepts
other UUID versions. Keep the key and request body for retries. The advertised window starts when
you first send the request.

Idempotency is off by default. Administrators can enable it with `polaris.idempotency.enabled`
and set the retry window with `polaris.idempotency.ttl` (default five minutes).

For an update, include `assert-table-uuid` with the UUID of the table you loaded. This prevents
a retry from changing a different table that later takes the same name.

If your update depends on a branch, include its expected snapshot ID. Suppose you load `main`
at snapshot `100` and prepare an append. Set `assert-ref-snapshot-id` for
`main` to `100`. If another writer advances `main` before your append commits, Polaris rejects
the stale update. When creating a branch, use `null` to require that the name is still free.

## Can I retry after a timeout?

If the original request had no key, the catalog omitted `idempotency-key-lifetime`, or the window
has expired, **do not retry automatically**. [Check what committed](#check-what-committed) first.
For a supported operation within the window, use the original request body and key.

* **Update an existing table.** If your request committed a change, the retry can return newer
  metadata from another writer. An update that changes no metadata records no key.
  If the original request lacked the table or branch requirement and the table or branch may
  have changed, check what committed before retrying.
* **Create a table directly** (`stage-create: false`). If someone may have renamed or dropped the
  table, check first. Polaris stores the key on the table. Once the name is free, a retry can create
  another table under that name. If the retry overlaps the first request, it can
  return `409 Conflict` even when the first request succeeds.
* **Finish a staged create.** A retry checks `assert-create` again. If the first request created
  the table, the retry can return `409 Conflict`. For either kind of create, a conflict alone
  leaves the outcome uncertain.

## Check what committed

**For a create**, reload the table. If you have the UUID from a staged create, compare it with
the loaded table's UUID. The table name alone cannot tell you which request created it.

**For an append**, load the table with `snapshots=all`. Start at the target branch's current
snapshot and follow its parents. Finding your original snapshot ID confirms the append, even if
another writer has advanced the branch. An expired snapshot can leave that history incomplete.

If the outcome is still unclear, keep the files and investigate before writing again. Rebuilding
the append with a new snapshot ID can add the same data files twice.

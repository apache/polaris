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
title: Promoting Changes Across Tables
type: docs
weight: 440
---

Some changes only make sense across several tables at once: a backfill that rewrites related
fact tables, a migration that changes dimensions and facts together, or a daily load that is
checked before anyone may read it (write-audit-publish). If the tables are updated one by one,
readers can see a mix of old and new data, and a failure halfway through leaves the tables
inconsistent.

Polaris can make such a change visible on all tables at once, using Iceberg branches and the
multi-table commit endpoint of the Iceberg REST API, `POST /v1/{prefix}/transactions/commit`. A
multi-table commit applies the changes to every table in the request, or to none of them.

No Polaris-specific feature is involved: the steps below use standard Iceberg table branches, and
work with any client that can send a multi-table commit.

## The steps

The examples use a catalog `lake` with tables `sales.orders` and `sales.items`, and a branch named
`backfill`.

### 1. Create the branch on every table

Load each table and note the snapshot `main` points to. Then create the branch on all tables in
one commit. The `assert-ref-snapshot-id` requirement makes the commit fail if `main` of any table
changed in the meantime.

```json
POST /api/catalog/v1/lake/transactions/commit

{
  "table-changes": [
    {
      "identifier": {"namespace": ["sales"], "name": "orders"},
      "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 1001}],
      "updates": [{"action": "set-snapshot-ref", "ref-name": "backfill", "type": "branch", "snapshot-id": 1001}]
    },
    {
      "identifier": {"namespace": ["sales"], "name": "items"},
      "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 2001}],
      "updates": [{"action": "set-snapshot-ref", "ref-name": "backfill", "type": "branch", "snapshot-id": 2001}]
    }
  ]
}
```

### 2. Write to the branch

Engines write to an Iceberg branch as usual, and readers of `main` don't see those writes. For
example, in Spark:

```sql
INSERT INTO lake.sales.orders.branch_backfill SELECT ...;
```

Validate the branch before promoting it, for example by querying
`lake.sales.orders.branch_backfill`.

### 3. Promote the branch

Load each table again and note the snapshot of `main` (unchanged since step 1, if nobody else wrote
to it) and of `backfill`. Then move `main` of every table to its branch head in one commit:

```json
POST /api/catalog/v1/lake/transactions/commit

{
  "table-changes": [
    {
      "identifier": {"namespace": ["sales"], "name": "orders"},
      "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 1001}],
      "updates": [{"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": 1005}]
    },
    {
      "identifier": {"namespace": ["sales"], "name": "items"},
      "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 2001}],
      "updates": [{"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": 2007}]
    }
  ]
}
```

Readers now see the new state of every table. If another writer changed `main` of any of the
tables after you read it, the commit fails with `409 Conflict` and none of the tables change.

Only move `main` forward: the branch should have been created from the snapshot `main` still
points to, so the new snapshot of `main` contains everything that was in the old one. If `main`
has moved since the branch was created, rebase or redo the work on the branch first.

### 4. Remove the branch

Optionally remove the branch from every table, again in one commit, with a
`{"action": "remove-snapshot-ref", "ref-name": "backfill"}` update per table. Snapshot expiry then
cleans up as usual.

## What this does not cover

These limits come from Iceberg table branches:

* **Schema changes are table-wide.** A schema change made while writing to the branch becomes the
  table's current schema for readers of `main` too, before the branch is promoted.
* **Only existing tables can be branched.** Tables, views or namespaces can't be created or dropped
  "on the branch".
* **The client keeps track of the tables.** Polaris doesn't know which tables belong to a branch;
  finding them means loading each table.

## Notes

* A multi-table commit needs both `TABLE_WRITE_PROPERTIES` and `TABLE_CREATE` on every table in the
  request (or a broader privilege that includes them).
* Multi-table commits are not supported on federated catalogs or static-facade catalogs.
* The commit checks each table it writes against the version it read, in a single write, so a
  concurrent change to any of those tables fails the whole commit. The steps above only update
  table refs, so they don't depend on any other entity.
* Very large commits (thousands of tables) are one large write to the metastore; consider promoting
  in groups that must be consistent with each other rather than one request for everything.

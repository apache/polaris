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
title: 'Recovering from an unknown Iceberg table commit'
linkTitle: 'Table commit retries'
type: docs
weight: 210
---

A timeout can leave an Iceberg client unable to tell whether Polaris committed a write.
The same applies when the service reports that the commit state is unknown. Keep the data and
metadata files from the attempt while you establish the outcome. A successful commit may already
reference those files.

## Retrying a request with an idempotency key

In a native Polaris catalog, the following steps apply to a direct table create
(`stage-create: false`) or an update to an existing table. The final commit of a staged create
checks `assert-create` again on retry and can return `409 Conflict` even if the first commit succeeded.

Check the catalog configuration before the first request. For example, for the `analytics`
catalog, call `GET /api/catalog/v1/config?warehouse=analytics`. If the response contains
`idempotency-key-lifetime`, it gives the retry window measured from the first submission.
Polaris disables this feature by default. When enabled with `polaris.idempotency.enabled`,
the default window is five minutes (`polaris.idempotency.ttl`).

1. Before the first request, generate a UUIDv7 and send it in the `Idempotency-Key` header.
   Generate a fresh key for each new logical operation.
2. Before retrying a create, check whether the table may have been renamed or dropped since the
   first request. If so, establish what happened first. Polaris stores the key on the table.
   Once the original name is free, retrying can create a new table even within the retry window.
3. To retry within the window, resend the same request with the same key. If the original
   request committed, the response can reflect newer table metadata, including changes another
   writer committed in the meantime.

## Checking the table when a retry cannot establish the outcome

If the server omitted `idempotency-key-lifetime`, the original request had no key, or its
window has expired, reload the table before deciding whether to write again. For a table
create, the name alone cannot establish which request created the table.

For an append, load the table with `snapshots=all` and look for the original snapshot ID in the
history of the branch you wrote to. Finding it confirms the append, even if another writer has
since advanced the branch. A missing snapshot is inconclusive because snapshots can expire.

If the outcome is still uncertain, retain the files and investigate before issuing another write.
Rebuilding an append with a new snapshot ID can add the same data files twice.

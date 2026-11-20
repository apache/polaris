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

For a direct table create or a single table update in a native Polaris catalog, check the
`GET /api/catalog/v1/config` response for `idempotency-key-lifetime`. If present, it gives the
retry window, measured from the first submission. Polaris disables this feature by default.
When enabled with `polaris.idempotency.enabled`, the default window is five minutes
(`polaris.idempotency.ttl`).

1. Before the first request, generate a UUIDv7 and send it in the `Idempotency-Key` header.
2. If the outcome is unknown and the window is still open, resend the same request with the same
   key. If the original request committed, the response can reflect newer table metadata, including
   changes another writer committed in the meantime.
3. Generate a new key for each new logical operation.

## Checking the table when a retry cannot establish the outcome

If the server omitted `idempotency-key-lifetime`, the original request had no key, or its
window has expired, reload the table before deciding whether to write again. For an append, look
for the snapshot ID from the original request in the history of the branch you wrote to. Finding
that snapshot confirms the append, even if another writer has since advanced the branch. For a
table create, the name alone cannot establish which request created the table.

If the outcome is still uncertain, retain the
files and investigate before issuing another write. Sending the append with a new key can apply
the same logical change twice.

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
title: Configuring Cloudflare R2 Storage
linkTitle: Configuring Cloudflare R2 Storage
type: docs
weight: 630
---

Cloudflare R2 speaks the S3 API. Polaris models it as its own storage type, `R2`, because its
credential model differs from AWS: there is no STS. Polaris mints **temporary credentials** itself
by signing a JWT with a server-side **parent token**, the path Cloudflare documents for
high-volume minting. Each vended credential is bound to one bucket and to the table's key
prefixes, and it expires after `STORAGE_CREDENTIAL_DURATION_SECONDS`.

## Server configuration

Create an R2 API token in the Cloudflare dashboard with **Object Read & Write** on every bucket
your catalogs use; a temporary credential cannot exceed its parent's permissions. Give Polaris
its access key id and secret:

```properties
polaris.storage.r2.access-key=<parent access key id>
polaris.storage.r2.secret-key=<parent secret access key>
```

To use different parent tokens for different catalogs, name them and reference the name from the
catalog's `storageName`:

```properties
polaris.storage.r2.research.access-key=...
polaris.storage.r2.research.secret-key=...
```

Deliver the secret through a secret config source or environment variables
(`POLARIS_STORAGE_R2_SECRET_KEY`), not a checked-in properties file. Avoid a storage name equal to
`access-key` or `secret-key`; it would collide with the default entry. Unlike S3, R2 has no ambient
credential chain, so a static server-side parent token is the only supported path. Polaris logs a
warning at startup if only one half of the key pair is set.

Add `R2` to `SUPPORTED_CATALOG_STORAGE_TYPES` if your deployment overrides the default list.

## Catalog configuration

```json
{
  "storageType": "R2",
  "accountId": "0123456789abcdef0123456789abcdef",
  "jurisdiction": "eu",
  "allowedLocations": ["s3://my-bucket/warehouse/"],
  "storageName": "research"
}
```

- `accountId` (required): the Cloudflare account id, 32 lowercase hex characters. The endpoint
  `https://<accountId>.r2.cloudflarestorage.com` and the audience of vended credentials derive
  from it.
- `jurisdiction` (optional): `eu`, `fedramp`, or `us`. Changes the host to
  `<accountId>.<jurisdiction>.r2.cloudflarestorage.com`. A bucket's jurisdiction is fixed at
  creation, so this field cannot be changed on an existing catalog (nor can `accountId`) unless
  `ALLOW_UNRESTRICTED_STORAGE_CONFIG_ROLE_CHANGES` is enabled.
- `allowedLocations`: `s3://` or `s3a://` locations. A catalog may span buckets, but one table's
  locations must sit in one bucket: R2 credentials cannot span buckets. Lay tables out
  bucket-per-table or bucket-per-namespace.

Catalog creation fails with 400 when the referenced parent token is not configured.

## What clients receive

With `X-Iceberg-Access-Delegation: vended-credentials`, `loadTable` returns
`s3.access-key-id` (the parent key id), `s3.secret-access-key`, `s3.session-token`,
`s3.session-token-expires-at-ms`, `s3.endpoint`, `s3.path-style-access=true`,
`client.region=auto`, and `client.refresh-credentials-endpoint`. Every client that can load a
table learns the parent key id; the secret and token are per table. For the full property reference
and client compatibility, see
[Vended Credentials Reference — Cloudflare R2]({{% ref "../../vended-credentials#cloudflare-r2" %}}).

Clients that honor `client.refresh-credentials-endpoint` never observe expiry. Clients that do not
hold a credential valid for `STORAGE_CREDENTIAL_DURATION_SECONDS` from mint time. Polaris caches
vended credentials for `min(remaining lifetime / 2, STORAGE_CREDENTIAL_CACHE_DURATION_SECONDS)`;
the cache duration must stay below the credential duration or the first vend fails.

## Diagnosing access errors

R2 checks the signed token when data is accessed, not when Polaris vends it.

| Client symptom | Likely cause |
|---|---|
| `AccessDenied` on every object | parent token lacks permission on the bucket, or wrong `jurisdiction` (audience mismatch) |
| `AccessDenied` on objects under another table | expected: credentials are prefix-scoped |
| `InvalidToken` / signature errors | server clock skew — Polaris hosts need NTP |
| 400 `No default R2 parent token is configured` | set `polaris.storage.r2.*` or the catalog's `storageName` entry |
| 400 `R2 credentials are scoped to one bucket` | a table's locations span buckets |

## Rotating the parent token

Revoking a parent token invalidates every temporary credential derived from it immediately.
Polaris reads `polaris.storage.r2.*` at startup. To rotate: create the new token, deploy Polaris
with it, wait at least `STORAGE_CREDENTIAL_DURATION_SECONDS`, then revoke the old token.

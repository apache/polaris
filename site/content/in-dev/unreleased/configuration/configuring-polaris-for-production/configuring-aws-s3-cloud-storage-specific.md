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
title: Configuring AWS S3 Cloud Storage
linkTitle: Configuring AWS S3 Cloud Storage
type: docs
weight: 610
---

This page covers configuring AWS S3 as the storage backend for a Polaris catalog. All read and write
operations against S3 are performed using credential vending, in which Polaris assumes an IAM role
on behalf of the client and returns scoped, short-lived credentials. The IAM role, its trust policy,
and the bucket itself must be set up before the catalog is created.

## IAM role and trust policy

Polaris assumes a customer-managed IAM role via STS when a client requests credentials. The role
must:

1. Grant the actions required for object access on the bucket and prefix that backs the catalog
   (`s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` and, if encryption is in use,
   the relevant `kms:*` actions).
2. Trust the Polaris service principal — typically the IAM role that the Polaris server runs as.
   Polaris fills the `sts:AssumeRole` request with the configured `userArn` and, when supplied, an
   `externalId`. The trust policy must accept both.

A minimal trust policy looks like:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "AWS": "arn:aws:iam::123456789012:role/polaris-server" },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": { "sts:ExternalId": "polaris-prod" }
      }
    }
  ]
}
```

If you do not require an external ID, omit the `Condition` block and the matching `externalId`
field in the storage config.

## Catalog storage configuration

Provide the role ARN and region when creating the catalog. `userArn` is the identity Polaris
itself uses (typically the role ARN of the server); `externalId` matches the trust policy above.

```bash
curl -X POST https://<polaris-host>/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "type": "INTERNAL",
        "name": "warehouse_s3",
        "storageConfigInfo": {
          "storageType": "S3",
          "roleArn": "arn:aws:iam::123456789012:role/polaris-warehouse-access",
          "userArn": "arn:aws:iam::123456789012:role/polaris-server",
          "externalId": "polaris-prod",
          "region": "us-east-1"
        },
        "properties": { "default-base-location": "s3://warehouse-bucket/prod/" }
      }'
```

The role ARN is validated against the pattern enforced by `AwsStorageConfigurationInfo`; an
ill-formed ARN is rejected at catalog creation time.

## Server-side encryption with KMS

When the bucket uses SSE-KMS, supply the key Polaris should use for writes and the full set of
keys it is allowed to read from:

```json
"storageConfigInfo": {
  "storageType": "S3",
  "roleArn": "...",
  "region": "us-east-1",
  "currentKmsKey": "arn:aws:kms:us-east-1:123456789012:key/aaaa-bbbb",
  "allowedKmsKeys": [
    "arn:aws:kms:us-east-1:123456789012:key/aaaa-bbbb",
    "arn:aws:kms:us-east-1:123456789012:key/cccc-dddd"
  ]
}
```

The IAM role's policy must include `kms:GenerateDataKey` and `kms:Decrypt` on every key listed in
`allowedKmsKeys`, and the key policy must grant the same to the role principal.

If the deployment does not use KMS, set `kmsUnavailable` to `true` so Polaris will not request
KMS-related session permissions:

```json
"kmsUnavailable": true
```

## S3-compatible endpoints

Polaris can be pointed at S3-compatible object stores (MinIO, Ceph RGW, Apache Ozone S3 gateway).
The available fields are:

- `endpoint` — the S3 API endpoint Polaris and its clients should call.
- `endpointInternal` — optional, used by the Polaris server when the in-cluster endpoint differs
  from the one returned to clients.
- `pathStyleAccess` — set to `true` for backends that do not support virtual-host-style addressing.
- `stsEndpoint` — STS endpoint; defaults to `endpointInternal` then `endpoint` when not set.
- `stsUnavailable` — set to `true` when the backend does not implement STS.

How clients receive credentials depends on whether the backend implements STS.

### Backends with STS support (e.g. AWS S3, MinIO)

Leave `stsUnavailable` unset (or `false`). Polaris will assume the role and vend short-lived,
subscoped credentials to the client at table-load time when the client sends
`X-Iceberg-Access-Delegation: vended-credentials`. This is the recommended deployment for AWS S3
and any compatible backend that exposes the STS API.

```json
"storageConfigInfo": {
  "storageType": "S3",
  "endpoint": "https://s3.internal.example.com",
  "pathStyleAccess": true,
  "region": "us-east-1"
}
```

### Backends without STS support (e.g. Apache Ozone S3 gateway, Ceph RGW without STS enabled)

Set `stsUnavailable: true`. Polaris will then skip subscoped credential vending, and clients must
authenticate to the object store directly with long-lived credentials. Because the vended-credential
path is disabled, the client must omit the `X-Iceberg-Access-Delegation` header and supply its own
access key / secret to the underlying FileIO. The Polaris guides for [Apache Ozone][ozone-guide]
and [Ceph][ceph-guide] show this pattern.

```json
"storageConfigInfo": {
  "storageType": "S3",
  "endpoint": "https://s3.internal.example.com",
  "pathStyleAccess": true,
  "stsUnavailable": true,
  "region": "us-east-1"
}
```

[ozone-guide]: ../../../../guides/ozone/
[ceph-guide]: ../../../../guides/ceph/

## Client configuration

Engines connect through the Iceberg REST API and let Polaris vend credentials at table-load time;
they do not need static AWS credentials when STS is available.

Spark example, matching the property names used by the existing MinIO / RustFS guides:

```shell
bin/spark-sql \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,org.apache.iceberg:iceberg-aws-bundle:1.10.1 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.polaris.type=rest \
    --conf spark.sql.catalog.polaris.uri=https://<polaris-host>/api/catalog \
    --conf spark.sql.catalog.polaris.oauth2-server-uri=https://<polaris-host>/api/catalog/v1/oauth/tokens \
    --conf spark.sql.catalog.polaris.token-refresh-enabled=false \
    --conf spark.sql.catalog.polaris.warehouse=warehouse_s3 \
    --conf spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL \
    --conf spark.sql.catalog.polaris.credential=<client-id>:<client-secret> \
    --conf spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation=vended-credentials
```

The `oauth2-server-uri` is recommended: without it the Iceberg REST client falls back to a
hard-coded `/v1/oauth/tokens` path and logs a deprecation warning, since the automatic fallback
is slated for removal in a future Iceberg release.

For Trino, use the Iceberg connector with the REST catalog. Two groups of properties are
required: the REST/OAuth2 settings for talking to Polaris, and the native S3 filesystem settings
that Trino uses to read the vended credentials.

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://<polaris-host>/api/catalog
iceberg.rest-catalog.warehouse=warehouse_s3
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=<client-id>:<client-secret>
iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
iceberg.rest-catalog.oauth2.server-uri=https://<polaris-host>/api/catalog/v1/oauth/tokens
iceberg.rest-catalog.vended-credentials-enabled=true
fs.native-s3.enabled=true
s3.region=us-east-1
```

When pointing at an S3-compatible endpoint, also set:

```properties
s3.endpoint=https://s3.internal.example.com
s3.path-style-access=true
```

For PyIceberg, use the `rest` catalog type. The same Polaris-side properties (`uri`, `warehouse`,
`credential`, `scope`, `oauth2-server-uri`) apply, and the vended-credential header must be
forwarded as a REST header:

```python
from pyiceberg.catalog.rest import RestCatalog

cat = RestCatalog(
    name="polaris",
    **{
        "uri": "https://<polaris-host>/api/catalog",
        "warehouse": "warehouse_s3",
        "credential": "<client-id>:<client-secret>",
        "scope": "PRINCIPAL_ROLE:ALL",
        "oauth2-server-uri": "https://<polaris-host>/api/catalog/v1/oauth/tokens",
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
    },
)
```

Polaris returns the vended S3 properties (`s3.access-key-id`, `s3.secret-access-key`,
`s3.session-token`) to the client at table-load time, so static credentials should not be
configured on the PyIceberg side.

## Verifying the setup

A successful end-to-end test should be possible without giving the client any long-lived AWS
credentials:

```sql
CREATE NAMESPACE warehouse_s3.demo;
CREATE TABLE warehouse_s3.demo.t (id BIGINT, name STRING) USING iceberg;
INSERT INTO warehouse_s3.demo.t VALUES (1, 'hello');
SELECT * FROM warehouse_s3.demo.t;
```

If `INSERT` or `SELECT` fails with a 403, the most common causes are:

- The IAM role's trust policy does not match the `userArn` / `externalId` Polaris is presenting.
- The role grants S3 permissions but is missing the KMS actions for `currentKmsKey`.
- The bucket policy denies access from outside a specific VPC endpoint.

Polaris logs the assumed-role STS request at debug level, which is the fastest way to confirm
which identity is being presented.

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
title: Configuring S3 Storage
linkTitle: Configuring S3 Storage
type: docs
weight: 610
---

This page covers configuring AWS S3, and S3-compatible object stores (MinIO, Apache Ozone S3
gateway, Ceph RGW, and similar), as the storage backend for a Polaris catalog. On AWS S3, all read
and write operations are performed using credential vending: Polaris assumes a customer IAM role
via STS and returns scoped, short-lived credentials to the client. The IAM role, its trust policy,
and the bucket itself must be set up before the catalog is created.

This page is limited to native Polaris authentication. External identity providers are also
supported but are not yet covered here; the configuration patterns below remain otherwise the same.

## IAM identities involved

Three distinct IAM identities take part in the S3 credential-vending flow. Conflating them is the
most common source of `AccessDenied` errors at catalog creation or table load.

| # | Identity | Used by | Purpose |
|---|----------|---------|---------|
| 1 | Polaris service identity | Polaris server process | Sign every `sts:AssumeRole` request Polaris makes |
| 2 | Catalog access role | Polaris (assumed) | Hold the actual S3 / KMS permissions on the catalog bucket |
| 3 | Vended credentials | Iceberg client (Spark/Trino/PyIceberg) | Short-lived session keys returned by Polaris at table-load time |

At runtime the client calls Polaris to load a table, Polaris signs an `sts:AssumeRole` request
as identity 1 against identity 2, AWS STS returns scoped temporary credentials (identity 3), and
the client uses those credentials to talk to S3 and KMS directly.

Identity 1 is configured once at Polaris deployment time. Identity 2 is created per catalog and
its ARN is registered when the catalog is created. Identity 3 is generated on every table load
and is never persisted.

## Polaris service identity

The Polaris server itself needs an AWS identity to call STS on the catalog access role. This is
configured outside Polaris — through the standard AWS SDK credentials chain — and is independent
of any catalog.

Pick whichever discovery mechanism matches the deployment:

- **EKS / IRSA** — annotate the Polaris ServiceAccount with the IAM role ARN
  (`eks.amazonaws.com/role-arn`). The pod receives a projected token and exchanges it for STS
  credentials automatically.
- **EC2** — attach an IAM instance profile to the EC2 instance. The SDK reads credentials from
  IMDS.
- **Static credentials** — set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optionally
  `AWS_SESSION_TOKEN` and `AWS_REGION` in the Polaris container's environment. Suitable for local
  development only.

Any other AWS compute environment that participates in the standard AWS SDK credentials chain
should also work, though the patterns above are the ones we have validated.

Whichever mechanism is used, the resulting identity needs a single permission to talk to STS:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": "arn:aws:iam::123456789012:role/polaris-warehouse-access"
    }
  ]
}
```

`Resource` should list every catalog access role Polaris is expected to assume; use a wildcard
(for example `arn:aws:iam::123456789012:role/polaris-*`) only when role names follow a strict
naming convention that the AWS account owner controls.

The ARN of this identity is the `Principal` that the catalog access role's trust policy must
trust — see the next section. One easy mistake is to update the catalog role's permissions
without also adding the Polaris service identity to its trust policy; the symptom is
`STS:AssumeRole` returning `AccessDenied` even though the catalog role has correct S3
permissions.

For an end-to-end deployment example that exercises this identity on EC2, see
[Deploying Polaris on AWS]({{% relref "../../getting-started/deploying-polaris/cloud-deploy/deploy-aws" %}}).

## Catalog access role and trust policy

Polaris assumes a customer-managed IAM role via STS when a client requests credentials. The role
must:

1. Grant the actions required for object access on the bucket and prefix that backs the catalog
   (`s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` and, if encryption is in use,
   the relevant `kms:*` actions).
2. Trust the Polaris service principal — typically the IAM role that the Polaris server runs as.
   Polaris fills the `sts:AssumeRole` request with an `externalId` when one is configured. The
   trust policy must accept the same external ID.

Using `externalId` is recommended for cross-account or hosted Polaris deployments to mitigate the
confused-deputy problem. A minimal trust policy looks like:

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

## Catalog storage configuration

Provide the role ARN, region, and `externalId` when creating the catalog. The token in the
`Authorization` header below is the Polaris admin bearer token obtained from
`/api/catalog/v1/oauth/tokens` (see [Configuring Polaris for Production]({{% relref "." %}}) for
how to bootstrap and issue admin tokens).

```bash
curl -X POST https://<polaris-host>/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "catalog": {
          "type": "INTERNAL",
          "name": "warehouse_s3",
          "properties": { "default-base-location": "s3://warehouse-bucket/prod/" },
          "storageConfigInfo": {
            "storageType": "S3",
            "roleArn": "arn:aws:iam::123456789012:role/polaris-warehouse-access",
            "externalId": "polaris-prod",
            "region": "us-east-1"
          }
        }
      }'
```

The role ARN is validated against the pattern enforced by `AwsStorageConfigurationInfo`; an
ill-formed ARN is rejected at catalog creation time.

## Server-side encryption with KMS

When the bucket uses SSE-KMS, supply both `currentKmsKey` (the key Polaris should use for writes)
and `allowedKmsKeys` (every key the catalog is allowed to read from). The two fields are processed
independently in `AwsCredentialsStorageIntegration`, so the write key must be included in
`allowedKmsKeys` as well if you want it readable through vended credentials:

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

The IAM role's policy must include `kms:GenerateDataKey` and `kms:Decrypt` on `currentKmsKey` and
`kms:Decrypt` on every key listed in `allowedKmsKeys`, and each key policy must grant the same to
the role principal.

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

The credential-vending guarantee at the top of this page assumes that the backend implements STS.
For AWS S3 and S3-compatible backends that expose the STS API (such as MinIO), leave
`stsUnavailable` unset (or `false`) and the vended-credentials flow described above works as is.

```json
"storageConfigInfo": {
  "storageType": "S3",
  "endpoint": "https://s3.internal.example.com",
  "pathStyleAccess": true,
  "region": "us-east-1"
}
```

For S3-compatible backends without STS (Apache Ozone S3 gateway, or Ceph RGW without STS enabled),
set `stsUnavailable: true`. Polaris will then skip subscoped credential vending entirely, and the
client must omit `X-Iceberg-Access-Delegation: vended-credentials` and authenticate to the object
store directly. The Polaris guides for [Apache Ozone][ozone-guide] and [Ceph][ceph-guide] show
this pattern end-to-end.

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

For Trino, use the Iceberg connector with the REST catalog. The REST/OAuth2 properties talk to
Polaris, and Polaris vends the endpoint, path-style flag, and region together with the scoped
credentials (`s3.endpoint`, `s3.path-style-access`, `client.region` in the load-table response),
so they do not need to be repeated on the client. The native S3 filesystem still has to be
enabled on the Trino side:

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

- The IAM role's trust policy does not match the `roleArn` / `externalId` Polaris is presenting.
- The role grants S3 permissions but is missing the KMS actions for `currentKmsKey`.
- The bucket policy denies access from outside a specific VPC endpoint.

Polaris logs the assumed-role STS request at debug level, which is the fastest way to confirm
which identity is being presented.

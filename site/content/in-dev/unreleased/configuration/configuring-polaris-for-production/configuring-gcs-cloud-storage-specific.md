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
title: Configuring GCS Cloud Storage
linkTitle: Configuring GCS Cloud Storage
type: docs
weight: 600
---

This page covers configuring Google Cloud Storage (GCS) as the storage backend for a Polaris
catalog. All catalog operations for GCS — listing, reading, and writing objects — go through
credential vending: Polaris takes the server's Google credentials, optionally impersonates a
catalog service account, downscopes the result with a Credential Access Boundary, and returns a
short-lived OAuth2 token to the Iceberg client at table-load time.

This page is limited to native Polaris authentication. External identity providers are also
supported but are not yet covered here; the configuration patterns below remain otherwise the same.

## Identities involved

Three identities take part in the GCS credential-vending flow. Mixing them up is a common
source of 403s at catalog creation or table load.

| # | Identity | Used by | Purpose |
|---|----------|---------|---------|
| 1 | Polaris service identity | Polaris server process | Ambient Application Default Credentials used to call IAM Credentials and downscope tokens |
| 2 | Catalog service account (optional) | Polaris (impersonated) | Hold the actual GCS permissions on the catalog bucket; set as `gcpServiceAccount` |
| 3 | Vended token | Iceberg client (Spark/Trino/PyIceberg) | Short-lived downscoped OAuth2 access token returned at table-load time |

At runtime the client calls Polaris to load a table, Polaris refreshes identity 1 (and, when
configured, impersonates identity 2 via `iamcredentials.googleapis.com`), builds a Credential
Access Boundary for the table's allowed locations, and returns identity 3. The client then talks
to GCS directly with `gcs.oauth2.token`.

Identity 1 is configured once at Polaris deployment time. Identity 2 is created per catalog and
its email is registered when the catalog is created. Identity 3 is generated on every table load
and is never persisted.

When `gcpServiceAccount` is omitted, Polaris downscopes identity 1 itself. That is convenient for
a single-project lab, but production catalogs should impersonate a dedicated service account so
the Polaris process identity does not need object-admin on every warehouse bucket.

## Polaris service identity

The Polaris server itself needs Google credentials that can create downscoped tokens, and that
can impersonate the catalog service account when one is configured. Credentials are loaded
through the standard Google ADC chain (`GoogleCredentials.getApplicationDefault()`), independent
of any catalog. The server then scopes them to
`https://www.googleapis.com/auth/cloud-platform`.

Pick whichever discovery mechanism matches the deployment:

- **GKE Workload Identity** — bind the Polaris Kubernetes ServiceAccount to a Google service
  account. The pod receives a projected token and ADC picks it up automatically.
- **GCE / Cloud Run / Cloud Functions** — attach a service account to the instance or service.
  ADC reads it from the metadata server.
- **Service-account key file** — set `GOOGLE_APPLICATION_CREDENTIALS` to a JSON key path in the
  Polaris container. Suitable for local development only; do not ship long-lived keys in
  production.

For an end-to-end VM deployment example, see
[Deploying Polaris on GCP]({{% relref "../../getting-started/deploying-polaris/cloud-deploy/deploy-gcp" %}}).

When a catalog sets `gcpServiceAccount`, identity 1 must be allowed to impersonate that account
(`roles/iam.serviceAccountTokenCreator` on the target, or an equivalent that includes
`iam.serviceAccounts.getAccessToken`). Without that grant, catalog operations fail with
`Unable to impersonate GCP service account`.

The impersonation token is requested with the
`https://www.googleapis.com/auth/devstorage.read_write` scope and a one-hour lifetime, matching
`GcpCredentialsStorageIntegration`.

## Catalog service account

Give the catalog service account (identity 2, or identity 1 if you skip impersonation) data-plane
access on the warehouse bucket and prefix. Polaris downscopes vended tokens to:

- `roles/storage.legacyObjectReader` on allowed read locations
- `roles/storage.objectViewer` on buckets that need list
- `roles/storage.legacyBucketWriter` on allowed write locations

The source identity still needs IAM that can actually perform those actions on the bucket; the
access boundary only narrows an existing grant. `roles/storage.objectAdmin` on the warehouse
prefix is the usual starting point.

## Hierarchical Namespace ACLs

Polaris requires both IAM roles and
[Hierarchical Namespace (HNS)](https://docs.cloud.google.com/storage/docs/hns-overview) ACLs (if
HNS is enabled) to be properly configured. Even with the correct IAM role (for example
`roles/storage.objectAdmin`), access to paths such as `gs://<bucket>/idsp_ns/sample_table4/` may
fail with 403 errors if HNS ACLs are missing for scoped tokens. The original access token may
work, but scoped (vended) tokens require HNS ACLs on the base path or relevant subpath.

HNS is not mandatory when using GCS for a catalog in Polaris. If HNS is not enabled on the
bucket, only IAM roles are required for access. Always verify HNS ACLs in addition to IAM roles
when troubleshooting GCS access issues with credential vending and HNS enabled.

## Catalog storage configuration

Provide the GCS location and optional catalog service account when creating the catalog. The
token in the `Authorization` header below is the Polaris admin bearer token obtained from
`/api/catalog/v1/oauth/tokens` (see [Configuring Polaris for Production]({{% relref "." %}}) for
how to bootstrap and issue admin tokens).

```bash
curl -X POST https://<polaris-host>/management/v1/catalogs \
  -H "Authorization: Bearer ***" \
  -H "Content-Type: application/json" \
  -d '{
        "catalog": {
          "type": "INTERNAL",
          "name": "warehouse_gcs",
          "properties": { "default-base-location": "gs://warehouse-bucket/prod/" },
          "storageConfigInfo": {
            "storageType": "GCS",
            "gcpServiceAccount": "polaris-warehouse@my-project.iam.gserviceaccount.com",
            "allowedLocations": [
              "gs://warehouse-bucket/prod/"
            ]
          }
        }
      }'
```

`default-base-location` and every `allowedLocations` entry must use the `gs://` scheme.
`GcpStorageConfigurationInfo` also records `gcpServiceAccount`; omit it only when the Polaris
process identity should be downscoped directly.

Iceberg I/O for GCS catalogs is `org.apache.iceberg.gcp.gcs.GCSFileIO`. Polaris returns that
implementation name with the vended credentials; engines still need the matching Iceberg GCP
jars on their own classpath.

## Client configuration

Engines connect through the Iceberg REST API and let Polaris vend credentials at table-load time;
they do not need a long-lived Google key on the client when vending is enabled.

Spark example, matching the property names used by the sibling S3 / Azure pages:

```shell
bin/spark-sql \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,org.apache.iceberg:iceberg-gcp-bundle:1.10.1 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.polaris.type=rest \
    --conf spark.sql.catalog.polaris.uri=https://<polaris-host>/api/catalog \
    --conf spark.sql.catalog.polaris.oauth2-server-uri=https://<polaris-host>/api/catalog/v1/oauth/tokens \
    --conf spark.sql.catalog.polaris.token-refresh-enabled=false \
    --conf spark.sql.catalog.polaris.warehouse=warehouse_gcs \
    --conf spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL \
    --conf spark.sql.catalog.polaris.credential=<client-id>:<client-secret> \
    --conf spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation=vended-credentials \
    --conf spark.sql.catalog.polaris.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO
```

The `oauth2-server-uri` is recommended: without it the Iceberg REST client falls back to a
hard-coded `/v1/oauth/tokens` path and logs a deprecation warning, since the automatic fallback
is slated for removal in a future Iceberg release.

The Spark application must include the Iceberg GCP bundle (or `iceberg-gcp` and the matching
Google Cloud jars) on the classpath; Polaris does not ship these jars to the engine.

For Trino, use the Iceberg connector with the REST catalog:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://<polaris-host>/api/catalog
iceberg.rest-catalog.warehouse=warehouse_gcs
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=<client-id>:<client-secret>
iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
iceberg.rest-catalog.oauth2.server-uri=https://<polaris-host>/api/catalog/v1/oauth/tokens
iceberg.rest-catalog.vended-credentials-enabled=true
```

For PyIceberg, use the `rest` catalog type and forward the vended-credential header as a REST
header:

```python
from pyiceberg.catalog.rest import RestCatalog

cat = RestCatalog(
    name="polaris",
    **{
        "uri": "https://<polaris-host>/api/catalog",
        "warehouse": "warehouse_gcs",
        "credential": "<client-id>:<client-secret>",
        "scope": "PRINCIPAL_ROLE:ALL",
        "oauth2-server-uri": "https://<polaris-host>/api/catalog/v1/oauth/tokens",
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
    },
)
```

Polaris returns the vended GCS properties (`gcs.oauth2.token`, `gcs.oauth2.token-expires-at`) to
the client at table-load time, as defined in `StorageAccessProperty`. Static
`GOOGLE_APPLICATION_CREDENTIALS` should not be configured on the PyIceberg side for this flow.

## Verifying the setup

A successful end-to-end test should be possible without giving the client any long-lived Google
credentials:

```sql
CREATE NAMESPACE warehouse_gcs.demo;
CREATE TABLE warehouse_gcs.demo.t (id BIGINT, name STRING) USING iceberg;
INSERT INTO warehouse_gcs.demo.t VALUES (1, 'hello');
SELECT * FROM warehouse_gcs.demo.t;
```

If `INSERT` or `SELECT` fails with a 403, the most common causes are:

- The Polaris service identity cannot impersonate `gcpServiceAccount` (`iam.serviceAccounts.getAccessToken` missing).
- The catalog service account (or the process identity, if impersonation is skipped) lacks
  `roles/storage.objectAdmin` — or an equivalent that covers read, list, and write — on the
  warehouse prefix.
- HNS is enabled on the bucket and the directory ACL does not include the scoped token's
  identity. IAM can look correct while the vended token still gets 403 on the table path.
- The client omitted `X-Iceberg-Access-Delegation: vended-credentials`, so Iceberg never received
  `gcs.oauth2.token`.

Polaris logs impersonation failures and access-boundary construction at error / debug level,
which is the fastest way to confirm which identity is being presented.

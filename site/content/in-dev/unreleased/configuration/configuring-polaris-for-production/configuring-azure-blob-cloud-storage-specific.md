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
title: Configuring Azure Blob Cloud Storage
linkTitle: Configuring Azure Blob Cloud Storage
type: docs
weight: 620
---

This page covers configuring Azure Blob Storage and Azure Data Lake Storage Gen2 (ADLS Gen2) as
the storage backend for a Polaris catalog. Polaris authenticates against Azure with the credentials
of a service principal that has data-plane access to the target storage account, and then vends
short-lived SAS tokens to clients on each table-load request.

## Service principal and Polaris credentials

Polaris uses the Azure SDK's `DefaultAzureCredential` chain, which by default reads the
service-principal credentials from environment variables. Create a service principal with data
access to the storage account and pass its credentials to the Polaris process:

```bash
# Replace <subscription>, <resource-group>, <storage-account> with your values.
az ad sp create-for-rbac \
  --name polaris-storage \
  --role "Storage Blob Data Contributor" \
  --scopes "/subscriptions/<subscription>/resourceGroups/<resource-group>/providers/Microsoft.Storage/storageAccounts/<storage-account>"
```

The command prints `appId`, `password`, and `tenant`. Set these on the Polaris server:

```bash
export AZURE_TENANT_ID=<tenant>
export AZURE_CLIENT_ID=<appId>
export AZURE_CLIENT_SECRET=<password>
```

In a Kubernetes deployment, do not embed `AZURE_CLIENT_SECRET` in the pod spec as plain text.
Store the client secret in a Kubernetes `Secret` (or an external secret store referenced by an
operator like the Azure Key Vault provider) and project it into the Polaris container with
`envFrom`/`valueFrom: secretKeyRef`. The same applies to the bootstrap credentials in
`POLARIS_BOOTSTRAP_CREDENTIALS`.

The `Storage Blob Data Contributor` role at storage-account scope lets Polaris issue SAS tokens
for any container under that account; scope the role narrower (single container) when you want to
confine a single Polaris catalog to one container.

## Storage account requirements

The storage account that backs the catalog should be configured with:

- **Hierarchical namespace (HNS)** is not strictly required by Polaris or Iceberg for table
  operations themselves. Its main effect is on how narrowly Polaris can scope a vended SAS token:
  with HNS enabled, Polaris can downscope a token to the directory (folder) that backs the
  requested namespace or table; without HNS the token can only be scoped at the container level.
  Production deployments that need per-namespace isolation should enable HNS.
- The `hierarchical` field in `storageConfigInfo` must match the actual state of the storage
  account. If they disagree, vended tokens are scoped against directory ACLs that do not exist
  on the storage side and runtime access errors result.
- A **container** that will hold the catalog's namespaces and tables (for example `warehouse`).
  Polaris does not create the container itself.
- **Firewall** rules that permit traffic from the Polaris control plane and from the engines that
  will read the data. SAS tokens do not bypass storage-account firewalls.

## Catalog storage configuration

With the service principal in place on the server, create the catalog with the storage account's
tenant ID and the `abfss://` location:

```bash
curl -X POST https://<polaris-host>/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "catalog": {
          "type": "INTERNAL",
          "name": "warehouse_azure",
          "properties": {
            "default-base-location": "abfss://warehouse@example.dfs.core.windows.net/prod/"
          },
          "storageConfigInfo": {
            "storageType": "AZURE",
            "tenantId": "00000000-0000-0000-0000-000000000000",
            "hierarchical": true,
            "allowedLocations": [
              "abfss://warehouse@example.dfs.core.windows.net/"
            ]
          }
        }
      }'
```

`default-base-location` must use the `abfss://` scheme together with the ADLS Gen2 endpoint
(`<account>.dfs.core.windows.net`). The `wasbs://` scheme is not supported.

`AzureStorageConfigurationInfo` also accepts `multiTenantAppName` and `consentUrl`. Current
Apache Polaris code does not use these fields when communicating with Azure APIs; they are
informational and can be omitted in self-hosted deployments that authenticate with a service
principal as described above.

## SAS token scoping and HNS ACLs

When HNS is enabled (`hierarchical: true`), Polaris narrows each vended SAS token to the directory
that backs the requested namespace or table. The ADLS Gen2 ACL on that directory must include the
service principal as well as any extra principals that should read the data outside of vended
credentials.

A common failure mode is a token that grants object-level permissions but is denied by a
directory-level ACL. The 403 returned by ADLS includes the path that was denied; align the ACL on
that exact prefix to recover.

When HNS is disabled, set `hierarchical: false`. Polaris will then issue SAS tokens scoped at the
container level rather than per-directory; this is acceptable for small deployments but does not
restrict cross-namespace access.

## Client configuration

Engines call Polaris through the Iceberg REST API and receive SAS-token properties at table-load
time; they do not need static Azure credentials on the client side.

Spark example, matching the property names used by the existing MinIO / RustFS guides:

```shell
bin/spark-sql \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,org.apache.iceberg:iceberg-azure-bundle:1.10.1 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.polaris.type=rest \
    --conf spark.sql.catalog.polaris.uri=https://<polaris-host>/api/catalog \
    --conf spark.sql.catalog.polaris.oauth2-server-uri=https://<polaris-host>/api/catalog/v1/oauth/tokens \
    --conf spark.sql.catalog.polaris.token-refresh-enabled=false \
    --conf spark.sql.catalog.polaris.warehouse=warehouse_azure \
    --conf spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL \
    --conf spark.sql.catalog.polaris.credential=<client-id>:<client-secret> \
    --conf spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation=vended-credentials \
    --conf spark.sql.catalog.polaris.io-impl=org.apache.iceberg.azure.adlsv2.ADLSFileIO
```

The `oauth2-server-uri` is recommended: without it the Iceberg REST client falls back to a
hard-coded `/v1/oauth/tokens` path and logs a deprecation warning, since the automatic fallback
is slated for removal in a future Iceberg release.

The Spark application must include the Iceberg Azure bundle (or `iceberg-azure` and the matching
Hadoop / Azure jars) on the classpath; Polaris does not ship these jars to the engine.

For Trino, use the Iceberg connector with the REST catalog, in the same shape as the AWS S3
example on the sibling page:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://<polaris-host>/api/catalog
iceberg.rest-catalog.warehouse=warehouse_azure
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=<client-id>:<client-secret>
iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
iceberg.rest-catalog.oauth2.server-uri=https://<polaris-host>/api/catalog/v1/oauth/tokens
```

For PyIceberg, use the `rest` catalog type and forward the vended-credential header as a REST
header:

```python
from pyiceberg.catalog.rest import RestCatalog

cat = RestCatalog(
    name="polaris",
    **{
        "uri": "https://<polaris-host>/api/catalog",
        "warehouse": "warehouse_azure",
        "credential": "<client-id>:<client-secret>",
        "scope": "PRINCIPAL_ROLE:ALL",
        "oauth2-server-uri": "https://<polaris-host>/api/catalog/v1/oauth/tokens",
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
    },
)
```

Polaris vends per-account SAS tokens to the FileIO at table-load time. The credentials are
returned under keys of the form `adls.sas-token.<storage-account>` (with optional
`dfs.core.windows.net` / `blob.core.windows.net` suffixes when scoping requires it), as defined in
`StorageAccessProperty`. The PyIceberg client picks these up directly; no static account key or
SAS token needs to be configured.

## Verifying the setup

```sql
CREATE NAMESPACE warehouse_azure.demo;
CREATE TABLE warehouse_azure.demo.t (id BIGINT, name STRING) USING iceberg;
INSERT INTO warehouse_azure.demo.t VALUES (1, 'hello');
SELECT * FROM warehouse_azure.demo.t;
```

If any operation fails:

- Errors from Azure AD (`AADSTS*`) usually mean the service principal credentials in
  `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` / `AZURE_TENANT_ID` are wrong, the secret has expired,
  or the principal has no role assignment on the storage account.
- `Failed to get subscoped credentials` with `Status code 403` and an
  `AuthorizationPermissionMismatch` Azure error body means the service principal does not have a
  data-plane role (`Storage Blob Data Contributor` or equivalent) at a scope that covers the path
  being accessed. Granting the role on the storage account — or the specific container — and
  waiting for RBAC propagation resolves it.
- 403 from `dfs.core.windows.net` paths with no Azure error code typically points at an HNS ACL
  mismatch on the base location when HNS is enabled and a narrow ACL is in place on the
  directory.
- 404 on container-level operations indicates that the container does not yet exist; Polaris does
  not create containers, only directories and blobs underneath them.

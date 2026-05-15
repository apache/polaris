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
the storage backend for a Polaris catalog. Polaris vends short-lived SAS tokens to clients after
authenticating itself against a customer Azure AD tenant; the multi-tenant application that
represents Polaris must be admitted to that tenant before catalog operations succeed.

## Tenant admission

Polaris runs as a multi-tenant Azure AD application. Before it can vend credentials for a bucket
in a customer tenant, an Azure AD administrator of that tenant must consent to the application.
On catalog creation Polaris returns a `consentUrl` that points at the standard Microsoft consent
endpoint; opening that URL with an account that has tenant-admin privileges grants the application
the role assignments required to read and write the storage account.

The same fields are surfaced by `AzureStorageConfigurationInfo`:

- `tenantId` — the Azure AD tenant that owns the storage account.
- `multiTenantAppName` — the application identifier Polaris will use; provided by your Polaris
  deployment.
- `consentUrl` — the URL the tenant admin must visit.

Until consent has been granted, `loadTable` and similar operations will fail with an Azure AD
`AADSTS65001` or `AADSTS700016` error.

## Storage account requirements

The storage account that backs the catalog should be configured with:

- **Hierarchical namespace (HNS)** enabled — this turns the account into ADLS Gen2 and is required
  for directory-aware operations (rename, recursive list) that Iceberg relies on for atomic
  metadata commits. If HNS is disabled, set `hierarchical: false` so Polaris will request flat-blob
  permissions only and avoid scoping SAS tokens to non-existent directory ACLs.
- **Storage Blob Data Contributor** role granted to the Polaris application on the account (or on
  the specific container if you want to scope access narrowly). Polaris cannot vend a token wider
  than the role assignment it holds.
- **Firewall** rules that permit traffic from the Polaris control plane and from the engines that
  will read the data. SAS tokens do not bypass storage-account firewalls.

## Catalog storage configuration

```bash
curl -X POST https://<polaris-host>/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "type": "INTERNAL",
        "name": "warehouse_azure",
        "storageConfigInfo": {
          "storageType": "AZURE",
          "tenantId": "00000000-0000-0000-0000-000000000000",
          "multiTenantAppName": "polaris-storage",
          "hierarchical": true
        },
        "properties": {
          "default-base-location": "abfss://warehouse@example.dfs.core.windows.net/prod/"
        }
      }'
```

The response includes `consentUrl`. Send it to the tenant administrator and wait for consent
before issuing further requests against the catalog.

`default-base-location` must use the `abfss://` scheme together with the ADLS Gen2 endpoint
(`<account>.dfs.core.windows.net`). The `wasbs://` scheme is not supported.

## SAS token scoping and HNS ACLs

When HNS is enabled (`hierarchical: true`), Polaris narrows each vended SAS token to the directory
that backs the requested namespace or table. The ADLS Gen2 ACL on that directory must include the
Polaris application as well as any extra principals that should read the data outside of vended
credentials.

A common failure mode is a token that grants object-level permissions but is denied by a
directory-level ACL. The 403 returned by ADLS includes the path that was denied; align the ACL on
that exact prefix to recover.

When HNS is disabled, set `hierarchical: false`. Polaris will then issue SAS tokens scoped at the
container level rather than per-directory; this is acceptable for small deployments but does not
restrict cross-namespace access.

## Client configuration

Engines call Polaris through the Iceberg REST API and receive SAS-token properties at table-load
time; they do not need static Azure credentials when consent is in place.

Spark with the Azure file-system driver:

```properties
spark.sql.catalog.warehouse_azure=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.warehouse_azure.type=rest
spark.sql.catalog.warehouse_azure.uri=https://<polaris-host>/api/catalog
spark.sql.catalog.warehouse_azure.warehouse=warehouse_azure
spark.sql.catalog.warehouse_azure.header.X-Iceberg-Access-Delegation=vended-credentials
spark.sql.catalog.warehouse_azure.io-impl=org.apache.iceberg.azure.adlsv2.ADLSFileIO
```

The Spark application must include `iceberg-azure` (or `iceberg-azure-bundle`) and
`hadoop-azure` on the classpath; Polaris itself does not ship these jars to the engine.

Trino:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://<polaris-host>/api/catalog
iceberg.rest-catalog.warehouse=warehouse_azure
iceberg.rest-catalog.vended-credentials-enabled=true
```

For PyIceberg, use the `rest` catalog type and rely on the `adls.sas-token` property returned by
Polaris.

## Verifying the setup

After consent has been granted:

```sql
CREATE NAMESPACE warehouse_azure.demo;
CREATE TABLE warehouse_azure.demo.t (id BIGINT, name STRING) USING iceberg;
INSERT INTO warehouse_azure.demo.t VALUES (1, 'hello');
SELECT * FROM warehouse_azure.demo.t;
```

If any operation fails:

- `AADSTS` errors usually mean consent has not been granted or `tenantId` is wrong.
- 403 from `dfs.core.windows.net` paths typically points at an HNS ACL mismatch on the base
  location.
- 404 on container-level operations indicates that the container does not yet exist; Polaris does
  not create containers, only directories and blobs underneath them.

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
title: Vended Credentials Reference
linkTitle: Vended Credentials Reference
type: docs
weight: 460
---

When an Iceberg client (Spark, Trino, PyIceberg, etc.) loads a table or view, Polaris can be
configured to return short-lived, scoped credentials that the client uses to access cloud storage
directly. These credentials are returned as key-value properties in the Iceberg REST API response
(see `LoadTableResult` and `LoadViewResult` in the [Iceberg REST
spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)).

Properties with kind `credential` carry sensitive material and are returned in the `credentials`
map of the API response (never logged). Properties with kind `config` carry non-sensitive
configuration and are returned in the `config` map.

There is no standard that defines common property names for vended credentials. The names Polaris
uses match what the Iceberg SDK expects for each storage type. This page documents the exact
property names Polaris emits so that client authors and operators can diagnose authentication
failures and understand client compatibility.

## AWS S3

Polaris calls [AWS STS `AssumeRole`](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html)
with an inline session policy scoped to the specific table locations and operations (read, list,
write) the caller is authorized to perform. The resulting temporary credentials are returned to the
client.

{{% include-config-section "storage-aws" %}}

## Azure ADLS

Polaris generates a
[User Delegation SAS token](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-user-delegation-sas-create-dotnet)
scoped to the container and path prefix the caller is authorized to access. Azure limits the
maximum validity of a User Delegation SAS token to **seven days**.

{{% include-config-section "storage-azure" %}}

{{< alert note >}}
The suffixed key forms (`adls.sas-token.<account-host>` and `adls.sas-token.<account-name>`) always
carry the same SAS token. Polaris emits all forms simultaneously so that different clients can each
find the key they expect without any client-specific configuration.
{{< /alert >}}

## Google Cloud Storage (GCS)

Polaris obtains a downscoped OAuth2 access token via Google's
[Credential Access Boundary](https://cloud.google.com/iam/docs/downscoping-short-lived-credentials)
mechanism. The token is scoped to the specific GCS buckets and path prefixes the caller is
authorized to access and supports optional service account impersonation.

{{% include-config-section "storage-gcs" %}}

## Credential refresh

When a client requests vended credentials by setting the `X-Iceberg-Access-Delegation` header to
`vended-credentials` on a table load or create request, Polaris also returns a credential-refresh
endpoint URL in the properties listed above (one per storage type). Iceberg clients that support
the Iceberg REST credential-refresh protocol can call this endpoint to obtain fresh credentials
before the current ones expire, avoiding the need to re-load the table.

## When delegation cannot be satisfied

`X-Iceberg-Access-Delegation` tells Polaris which access mechanisms the client supports. If the
header is present but the catalog can provide none of the requested mechanisms, Polaris fails the
request with `400 Bad Request` instead of returning the table without delegated access. The two
cases are:

| Requested mechanism | When it cannot be satisfied | Response |
|---|---|---|
| `vended-credentials` | The catalog cannot vend credentials for the table's storage, for example an S3-compatible store configured with `stsUnavailable: true`. | `400` with message `Credential vending was requested for table <namespace.table>, but no credentials are available` |
| `remote-signing`, alone or combined with `vended-credentials` when vending is not possible | Polaris does not implement remote signing. | `400` with message `This catalog cannot vend credentials or sign requests; request without X-Iceberg-Access-Delegation and configure storage credentials on the client` |

Polaris fails fast here on purpose. Both conditions are static properties of the catalog (its
storage configuration, or a mechanism Polaris does not support), so a later request for the same
table would not succeed either. A client that asks for delegation usually has no storage
credentials of its own, and a `200` response without credentials would only move the failure to
the first data access, where it is much harder to diagnose.

For catalogs that cannot vend credentials, configure the client to authenticate to the object
store directly and omit the header. In Spark, for example, remove
`spark.sql.catalog.<name>.header.X-Iceberg-Access-Delegation` and provide the storage credentials
through the usual Hadoop or Iceberg FileIO properties. The
[S3 storage page]({{% relref "./configuration/configuring-polaris-for-production/configuring-aws-s3-cloud-storage-specific#s3-compatible-endpoints" %}})
shows this for S3-compatible stores without STS.

## Client compatibility summary

The table below summarizes which property forms are required by common clients, based on the
Iceberg SDK each client uses.

| Client                             | Storage type | Required properties                                            |
|------------------------------------|--------------|----------------------------------------------------------------|
| Apache Spark (Iceberg ≥ 1.8)       | S3           | `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token` |
| Apache Spark (Iceberg ≥ 1.8)       | ADLS / Blob  | `adls.sas-token.<account-host>`                                |
| Apache Spark (Iceberg 1.7.x)       | ADLS / Blob  | `adls.sas-token.<account-name>`                                |
| PyIceberg (via `adlfs` / `fsspec`) | ADLS / Blob  | `adls.sas-token`, `adls.account-name`                          |
| Apache Spark / PyIceberg           | GCS          | `gcs.oauth2.token`                                             |

Polaris emits **all applicable forms simultaneously**, so no client-specific Polaris configuration
is required to switch between the clients listed above.

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
title: BigLake Federation
type: docs
weight: 707
---

Polaris can federate an external Apache Iceberg REST catalog backed by Google Cloud's Lakehouse
runtime catalog. This lets Polaris broker access to BigLake-managed Iceberg metadata while Polaris
continues to enforce its own authentication, RBAC, auditing, and storage-credential vending
behavior.

{{< alert note >}}
Google Cloud now refers to this service as the **Lakehouse runtime catalog**. Existing endpoint
hostnames, IAM role names, permission prefixes, and several API identifiers still use **BigLake**
naming. This guide uses "BigLake" when referring to Polaris configuration fields and Google API
identifiers, and "Lakehouse runtime catalog" when referring to the current Google product name.
{{< /alert >}}

{{< alert warning >}}
This guide documents verified Polaris behavior in the current unreleased branch. Do not treat it as
support for configurations that are not explicitly marked **Supported** in the compatibility matrix
below.
{{< /alert >}}

## Architecture and request flow

Polaris sits between the Iceberg client and Google Cloud:

1. A client authenticates to Polaris and calls the Polaris Iceberg REST endpoint.
2. Polaris resolves the external catalog and sends outbound Iceberg REST requests to BigLake at
   `https://biglake.googleapis.com/iceberg/v1/restcatalog`.
3. Polaris authenticates those outbound BigLake requests by using **server-side Google Application
   Default Credentials (ADC)**. This is separate from the client's Polaris token.
4. BigLake returns namespace and table metadata to Polaris.
5. If the client requests `X-Iceberg-Access-Delegation: vended-credentials`, Polaris uses its local
   GCS storage configuration to mint short-lived GCS credentials for the table location and returns
   them to the client.
6. The client uses those vended GCS credentials to read or write objects in Cloud Storage.

Important separation of duties:

- **BigLake authentication** controls how Polaris talks to the remote Iceberg REST endpoint.
- **GCS storage configuration** controls how Polaris vends storage access to clients.
- **BigLake does not issue GCS credentials to Polaris clients in this flow. Polaris does.**

## Feature configuration

Enable catalog federation in Polaris before creating a BigLake-backed external catalog:

```properties
# Enables federation itself
polaris.features."ENABLE_CATALOG_FEDERATION"=true

# These defaults already include the required values, but keep them if you maintain explicit allow-lists
polaris.features."SUPPORTED_CATALOG_CONNECTION_TYPES"=["ICEBERG_REST"]
polaris.features."SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES"=["OAUTH","BEARER","GCP","SIGV4"]

# Required for the tested GCS credential-vending flow
polaris.features."ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING"=true
polaris.features."ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING"=true
```

The last two flags are enabled by default. If either is disabled, Polaris will not support the
tested `vended-credentials` flow for a federated BigLake catalog.

## Runtime requirements

### BigLake endpoint and identifier rules

Polaris validates BigLake catalogs conservatively:

- `connectionConfigInfo.uri` must be exactly the HTTPS BigLake Iceberg REST endpoint:
  `https://biglake.googleapis.com/iceberg/v1/restcatalog`
- Custom ports, query strings, and fragments are rejected.
- `connectionConfigInfo.remoteCatalogName` must be one of:
  - A full BigLake catalog resource name such as
    `projects/my-project/locations/us/catalogs/analytics`
  - A simple catalog identifier such as `analytics`
  - A `gs://...` warehouse location

Polaris currently verifies the **catalog identifier** form in its cloud integration tests. The
`gs://...` warehouse form is accepted by validation but is not currently covered by the Polaris
BigLake cloud test suite.

### Quota and billing header

BigLake requires the `x-goog-user-project` header for quota and billing attribution. In the
Management API model, configure it in `connectionConfigInfo.properties` as:

```json
"properties": {
  "header.x-goog-user-project": "billing-project-id"
}
```

The value may be either a GCP project ID or a numeric project number.

### Google APIs, IAM, and storage prerequisites

At minimum, prepare the following on the Google Cloud side:

- Enable the Lakehouse / BigLake Iceberg REST catalog service for the target project as described in
  Google's [Lakehouse runtime catalog documentation](https://docs.cloud.google.com/lakehouse/docs/about-lakehouse-catalogs).
- Ensure the Polaris server can reach `biglake.googleapis.com` over HTTPS.
- Grant the **Polaris server identity** enough BigLake permissions for the operations you plan to
  allow, such as listing namespaces, listing tables, loading tables, and creating or updating table
  metadata.
- Grant the **Polaris GCS vending identity** enough Cloud Storage access for the catalog's default
  base location and allowed locations:
  - Read-only workflows need object read access.
  - Write workflows need object create/update/delete access as required by your engine.

Google publishes predefined BigLake roles such as
[`roles/biglake.viewer`, `roles/biglake.metadataViewer`, `roles/biglake.editor`, and `roles/biglake.admin`](https://docs.cloud.google.com/iam/docs/roles-permissions/biglake).
Choose the smallest role that contains the BigLake permissions your workflow needs.

For GCS access, use least-privilege bucket or prefix-level grants for the configured warehouse
locations rather than broad project-wide object access.

## Supported Google credential sources

For `authenticationType=GCP`, Polaris does **not** store Google credential material in the catalog
entity. Instead, it validates that Google Application Default Credentials are available on the
Polaris server and lets the Google authentication libraries obtain and refresh access tokens at
request time.

Supported server-side credential sources for this Polaris path are:

- `GOOGLE_APPLICATION_CREDENTIALS` pointing to a credential file on the Polaris server
- A local ADC file created by `gcloud auth application-default login` for development
- An attached service account on Google Cloud compute
- Workload identity / metadata-server-backed ADC on Google Cloud

See Google's official [Application Default Credentials documentation](https://docs.cloud.google.com/docs/authentication/application-default-credentials)
and [`gcloud auth application-default`](https://docs.cloud.google.com/sdk/gcloud/reference/auth/application-default)
reference for details.

{{< alert warning >}}
Do **not** store service-account JSON, private keys, refresh tokens, bearer tokens, or other
Google credential material in `catalog.properties`, `connectionConfigInfo.properties`, or CLI
arguments. Polaris explicitly rejects sensitive Google credential properties for GCP-authenticated
external catalogs.
{{< /alert >}}

## BigLake authentication versus GCS storage configuration

These two pieces of configuration solve different problems:

- `authenticationType=GCP` tells Polaris how to authenticate **outbound requests to BigLake**.
- `storageConfigInfo.storageType=GCS` and `storageConfigInfo.gcsServiceAccount` tell Polaris how to
  vend **Cloud Storage access for table data**.

In the Polaris CLI, this distinction appears as:

- `--catalog-authentication-type gcp` for BigLake REST authentication
- `--service-account ...` for the GCS service account used by Polaris storage configuration

The `--service-account` option does **not** change how Polaris authenticates to BigLake. It only
affects the GCS storage side.

## Management API example

The following request creates a BigLake-backed external catalog using the tested Polaris flow:

```bash
curl -X POST https://<polaris-host>/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "type": "EXTERNAL",
        "name": "analytics_biglake",
        "properties": {
          "default-base-location": "gs://analytics-warehouse/polaris/"
        },
        "storageConfigInfo": {
          "storageType": "GCS",
          "gcsServiceAccount": "polaris-gcs-vending@my-project.iam.gserviceaccount.com",
          "allowedLocations": [
            "gs://analytics-warehouse/polaris/"
          ]
        },
        "connectionConfigInfo": {
          "connectionType": "ICEBERG_REST",
          "uri": "https://biglake.googleapis.com/iceberg/v1/restcatalog",
          "remoteCatalogName": "projects/my-project/locations/us/catalogs/analytics",
          "properties": {
            "header.x-goog-user-project": "billing-project-id"
          },
          "authenticationParameters": {
            "authenticationType": "GCP"
          }
        }
      }'
```

Notes:

- Use `remoteCatalogName` for the BigLake catalog identifier or, for warehouse mode, a `gs://...`
  location.
- The `gcsServiceAccount` is required for the tested credential-vending path.
- Keep `default-base-location` and `allowedLocations` narrow and aligned with the remote catalog's
  warehouse layout.

## CLI example

The Polaris CLI supports `gcp` as an external catalog authentication type:

```bash
polaris catalogs create \
  --type external \
  --storage-type gcs \
  --default-base-location gs://analytics-warehouse/polaris/ \
  --allowed-location gs://analytics-warehouse/polaris/ \
  --service-account polaris-gcs-vending@my-project.iam.gserviceaccount.com \
  --catalog-connection-type iceberg-rest \
  --catalog-uri https://biglake.googleapis.com/iceberg/v1/restcatalog \
  --iceberg-remote-catalog-name projects/my-project/locations/us/catalogs/analytics \
  --catalog-authentication-type gcp \
  --property header.x-goog-user-project=billing-project-id \
  analytics_biglake
```

{{< alert note >}}
The current CLI exposes the quota-project header via the general `--property` mechanism instead of
a dedicated `connectionConfigInfo.properties` flag. That header is non-sensitive, and Polaris
currently merges it into the downstream BigLake REST client configuration. If you need exact
Management API field placement, use the Management API example above.
{{< /alert >}}

## Credential-vending flow

The tested BigLake data-access path uses Iceberg REST credential vending:

- The client sends `X-Iceberg-Access-Delegation: vended-credentials`.
- Polaris loads remote metadata from BigLake.
- Polaris evaluates the table locations against the external catalog's allowed GCS locations.
- Polaris issues short-lived GCS credentials through its normal storage-credential vending path.
- The client uses those vended GCS credentials for table reads and writes.

This means:

- **BigLake authenticates Polaris.**
- **Polaris vends GCS credentials to the client.**
- **The client does not need to bring separate GCS secrets when using the tested path.**

If credential vending is disabled for the external or federated catalog, this flow is no longer in
the supported configuration described by this guide.

## Client and engine examples

### Spark example

The following Spark properties match the BigLake REST endpoint and the tested Polaris
`vended-credentials` pattern:

```properties
spark.sql.defaultCatalog=analytics
spark.sql.catalog.analytics=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.analytics.type=rest
spark.sql.catalog.analytics.uri=https://<polaris-host>/api/catalog/v1
spark.sql.catalog.analytics.warehouse=analytics_biglake
spark.sql.catalog.analytics.token=<polaris-access-token>
spark.sql.catalog.analytics.header.X-Iceberg-Access-Delegation=vended-credentials
spark.sql.catalog.analytics.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

This example shows how a Spark client talks to **Polaris**, not directly to BigLake. Polaris then
federates to BigLake and vends GCS credentials back to Spark.

### Other Iceberg clients

Any Iceberg client that can:

- talk to a REST catalog,
- send a Polaris token,
- set `X-Iceberg-Access-Delegation: vended-credentials`, and
- use GCS FileIO

can be configured similarly.

Google documents direct Lakehouse runtime catalog integrations for Spark, Flink, Hive, and Trino in
its [Lakehouse runtime catalog documentation](https://docs.cloud.google.com/lakehouse/docs/about-lakehouse-catalogs),
but Polaris does **not** currently certify those client/engine combinations end-to-end in its own
BigLake cloud test suite.

## Supported operations

The current Polaris BigLake cloud integration suite verifies:

- Namespace create and namespace list
- Table create and table list
- Table load by identifier
- Repeated table loading within the same authenticated session
- Metadata refresh after a write
- Data write and read through a loaded Iceberg table
- Multiple sequential operations in the same session
- Long-running session behavior after Google token refresh
- Retry handling for supported transient failures
- No unnecessary retries for non-retryable failures

Operations outside that list should be treated as **not yet certified by Polaris for BigLake**
unless they are explicitly covered elsewhere in Polaris documentation.

## Compatibility matrix

| Polaris | Iceberg client | Engine / client path | BigLake mode | Storage credential mode | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Current unreleased branch | Apache Iceberg 1.11.0 RESTCatalog | Polaris BigLake cloud integration test harness | BigLake catalog identifier (`projects/.../catalogs/...` or equivalent configured catalog name) | Polaris vended GCS credentials | Supported | Verified namespace/table/load/read/write flow |
| Current unreleased branch | Apache Iceberg 1.11.0 RESTCatalog | Polaris BigLake cloud integration test harness | `gs://...` warehouse identifier | Polaris vended GCS credentials | Not tested | Accepted by validation, not covered by current cloud suite |
| Current unreleased branch | Spark configured against Polaris REST | Engine-driven production client | BigLake catalog identifier | Polaris vended GCS credentials | Not tested | Example provided, but not certified by Polaris CI |
| Current unreleased branch | Direct BigLake client against Google endpoint | Client bypasses Polaris | Catalog ID or `gs://...` | Client-managed Google storage auth | Unsupported by this guide | This guide covers federation **through Polaris** only |
| Current unreleased branch | Any Iceberg client | Polaris federation path | Any | Remote signing | Not tested | BigLake documentation in Polaris only covers `vended-credentials` |
| Current unreleased branch | Any Iceberg client | Polaris federation path | Any | Secrets embedded in catalog or connection properties | Unsupported | Polaris rejects sensitive Google credential properties |

## Known limitations

- BigLake federation in Polaris is currently documented only for **Iceberg REST** catalogs.
- The endpoint is fixed to `https://biglake.googleapis.com/iceberg/v1/restcatalog`; alternate
  hosts, custom ports, query strings, and fragments are rejected.
- The validated and cloud-tested path assumes **GCS storage** when credential vending is enabled.
- The Polaris BigLake guide currently certifies only the `vended-credentials` access-delegation
  flow.
- Google engine examples may support more combinations directly against the Lakehouse runtime
  catalog than Polaris has yet certified through federation.
- This guide does not cover generic-table federation for BigLake.

## Troubleshooting

Use the exception type, message, HTTP status, Google request ID, and metrics together. The
following failures are intentionally sanitized so operators can share them safely without exposing
tokens or private keys.

| Symptom | Polaris error type | What to check |
| --- | --- | --- |
| Polaris cannot load ADC at catalog create/update time | `BigLakeCredentialLoadingException` | Confirm ADC is available on the server through `GOOGLE_APPLICATION_CREDENTIALS`, a local ADC file, an attached service account, or workload identity |
| Polaris can load ADC but cannot mint a Google access token | `BigLakeTokenAcquisitionException` | Verify the server identity is valid and can obtain Google OAuth access tokens |
| BigLake returns 401 or the token is rejected | `BigLakeAuthenticationException` | Check the Polaris server identity, token scopes, and server-side ADC source |
| BigLake returns 403 | `BigLakeAuthorizationException` | Check BigLake IAM permissions on the remote catalog and Google API access for the Polaris server identity |
| BigLake rejects the billing/quota project | `BigLakeQuotaProjectException` | Verify `header.x-goog-user-project` is set to a valid billing project ID or number |
| Polaris cannot find the remote catalog or warehouse | `BigLakeCatalogConfigurationException` | Check `remoteCatalogName`, the endpoint URI, and remote IAM permissions |
| BigLake rejects an operation as unsupported | `BigLakeUnsupportedOperationException` | Check whether the operation is covered by the current support matrix |
| Timeouts, DNS failures, TLS failures, connection failures | `BigLakeTimeoutException`, `BigLakeDnsException`, `BigLakeTlsException`, `BigLakeConnectionException` | Check egress, DNS, proxy settings, TLS trust, and firewall rules to `biglake.googleapis.com` |
| Temporary remote outage or retryable 5xx | `BigLakeServiceUnavailableException` | Retry and inspect remote service health |
| Quota exhaustion or throttling | `BigLakeQuotaExceededException`, `BigLakeThrottledException` | Respect `Retry-After`, check billing, quota allocation, and request rates |
| GCS reads or writes fail after table load | GCS / FileIO failures in the client | Verify `gcsServiceAccount`, allowed locations, bucket IAM, and that the client requested `vended-credentials` |

## Logs and metrics

BigLake federation emits safe operational telemetry:

- Request counter: `polaris.federation.biglake.requests`
- Request latency timer: `polaris.federation.biglake.request.latency`

BigLake outbound request logs include fields such as:

- `catalog`
- `operation`
- `remoteHost`
- `responseStatus`
- `latencyMs`
- `retryCount`
- `failureCategory`
- `googleRequestId` when available
- `retryAfter` when available

The Google request ID is safe and useful when you need to correlate a Polaris failure with Google
Cloud support or audit logs.

## Operator notes

- **Credential rotation:** Rotate Google server-side credentials outside the catalog entity. For
  example, rotate the attached service account, workload identity binding, or the file referenced by
  `GOOGLE_APPLICATION_CREDENTIALS`.
- **Token refresh:** Polaris validates ADC availability up front and relies on Google's
  authentication libraries to acquire and refresh outbound access tokens automatically during
  requests.
- **GCS vending identity changes:** If you change `storageConfigInfo.gcsServiceAccount`, update the
  catalog configuration and re-validate table read/write flows.
- **Upgrade validation:** After Polaris, Iceberg, or Google authentication library upgrades, rerun
  the BigLake cloud integration suite and include the optional long-running refresh scenario.

For the current branch, the BigLake cloud test entry point is:

```bash
./gradlew :polaris-runtime-service:cloudTest --tests "org.apache.polaris.service.it.GcpCatalogFederationIntegrationIT"
```

If you want the long-running token-refresh scenario as part of that regression pass, set
`INTEGRATION_TEST_GCS_FEDERATED_REFRESH_WAIT_SECONDS` to a positive value before running the test.

## External references

- Google Cloud: [About the Lakehouse runtime catalog](https://docs.cloud.google.com/lakehouse/docs/about-lakehouse-catalogs)
- Google Cloud: [Set up the Lakehouse Iceberg REST catalog endpoint](https://docs.cloud.google.com/lakehouse/docs/set-up-lakehouse-iceberg-rest-catalog)
- Google Cloud: [Create a catalog](https://docs.cloud.google.com/lakehouse/docs/create-catalog)
- Google Cloud: [Application Default Credentials](https://docs.cloud.google.com/docs/authentication/application-default-credentials)
- Google Cloud: [gcloud auth application-default](https://docs.cloud.google.com/sdk/gcloud/reference/auth/application-default)
- Google Cloud: [BigLake roles and permissions](https://docs.cloud.google.com/iam/docs/roles-permissions/biglake)
- Apache Iceberg: [REST Catalog specification](https://iceberg.apache.org/rest-catalog-spec/)

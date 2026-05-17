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
title: BigQuery Metastore Federation
type: docs
weight: 706
---

Polaris can federate catalog operations to a BigQuery Metastore catalog. This lets BigQuery Metastore
remain the source of truth for Iceberg table metadata while Polaris brokers access, policies, and
multi-engine connectivity.

## Build-time enablement

The BigQuery factory is packaged as an optional extension and is not baked into default server
builds. Include it when assembling the runtime or container images by setting the `NonRESTCatalogs`
Gradle property to include `BIGQUERY` (and any other non-REST backends you need):

```bash
./gradlew :polaris-server:assemble :polaris-server:quarkusAppPartsBuild --rerun \
  -PNonRESTCatalogs=BIGQUERY -Dquarkus.container-image.build=true
```

`runtime/server/build.gradle.kts` wires the extension in only when this flag is present, so binaries
built without it will reject BigQuery federation requests.

## Feature configuration

After building Polaris with BigQuery Metastore support, enable the necessary feature flags in your
`application.properties` file (or equivalent configuration mechanism such as environment variables
or a Kubernetes ConfigMap):

```properties
# Allows BIGQUERY connection type
polaris.features."SUPPORTED_CATALOG_CONNECTION_TYPES"=["BIGQUERY"]

# Allows IMPLICIT authentication, needed for BigQuery Metastore federation
polaris.features."SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES"=["IMPLICIT"]

# Enables the federation feature itself
polaris.features."ENABLE_CATALOG_FEDERATION"=true
```

For Kubernetes deployments, add these properties to the ConfigMap mounted into the Polaris
container (typically at `/deployment/config/application.properties`).

## Runtime requirements

- **BigQuery API access:** The Polaris deployment must be able to reach the BigQuery API
  (`bigquery.googleapis.com`) over HTTPS.
- **Authentication:** BigQuery Metastore federation only supports `IMPLICIT` authentication, meaning
  Polaris uses Google Application Default Credentials (ADC) resolved at process startup. ADC is
  resolved from the `GOOGLE_APPLICATION_CREDENTIALS` environment variable, an attached service
  account on GCP compute, or local `gcloud` credentials during development. See Google's
  [Application Default Credentials documentation](https://cloud.google.com/docs/authentication/application-default-credentials)
  for details. Ensure valid credentials are available before starting the server.
- **IAM:** The identity used by Polaris must have read access to BigQuery datasets in the target
  project and read access to the GCS warehouse bucket (read/write if Polaris will commit table
  metadata).

## Creating a federated catalog

Use the Management API to create an external catalog whose connection type is `BIGQUERY`. The
following request registers a catalog that proxies to BigQuery Metastore in the GCP project
`my-gcp-project`, using `analytics_dataset` as the default warehouse:

```bash
curl -X POST https://<polaris-host>/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "type": "EXTERNAL",
        "name": "analytics_bigquery",
        "storageConfigInfo": {
          "storageType": "GCS"
        },
        "properties": {
          "default-base-location": "gs://analytics-bucket/warehouse/"
        },
        "connectionConfigInfo": {
          "connectionType": "BIGQUERY",
          "properties": {
            "gcp.bigquery.project-id": "my-gcp-project",
            "warehouse": "analytics_dataset"
          },
          "authenticationParameters": {
            "authenticationType": "IMPLICIT"
          }
        }
      }'
```

The `connectionConfigInfo.properties` map carries BigQuery-specific configuration consumed by
Iceberg's `BigQueryProperties`:

- `gcp.bigquery.project-id` (required): the GCP project that owns the BigQuery datasets.
- `warehouse` (required): the BigQuery dataset name used as the default warehouse for new tables.
- `gcp.bigquery.location` (optional): the BigQuery location (region) of the datasets.
- `gcp.bigquery.list-all-tables` (optional): when `true` (the default), `listTables` returns every
  table in a BigQuery dataset regardless of type. Set to `false` to filter to BigQuery-Metastore
  Iceberg tables only.

The following optional properties let Polaris assume a different service account for BigQuery
calls via service account impersonation:

- `gcp.bigquery.impersonate.service-account`: target service account email.
- `gcp.bigquery.impersonate.lifetime-seconds`: lifetime of the impersonated credential.
- `gcp.bigquery.impersonate.scopes`: OAuth scopes for the impersonated credential.
- `gcp.bigquery.impersonate.delegates`: chain of delegate service accounts.

## Limitations and operational notes

- **Single identity:** Because only `IMPLICIT` authentication is permitted, Polaris cannot mix
  multiple BigQuery identities in a single deployment
  (`BigQueryMetastoreFederatedCatalogFactory` rejects other auth types). Plan a deployment topology
  that aligns the Polaris process identity with the target project.
- **Generic tables:** The BigQuery extension exposes Iceberg tables registered in BigQuery
  Metastore. Generic table federation is not implemented
  (`BigQueryMetastoreFederatedCatalogFactory#createGenericCatalog` throws
  `UnsupportedOperationException`).
- **Mixed table types in listings:** By default `listTables` returns every table in a BigQuery
  dataset, including non-Iceberg tables. `loadTable` will return 404 for entries that are not
  Iceberg tables managed by BigQuery Metastore. Set `gcp.bigquery.list-all-tables=false` to filter.

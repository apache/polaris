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
title: Hadoop Catalog Federation
type: docs
weight: 706
---

Polaris can federate catalog operations to an Iceberg
[Hadoop catalog](https://iceberg.apache.org/docs/latest/java-api-quickstart/#using-a-hadoop-catalog):
a warehouse directory in which namespaces and tables are tracked by directory layout, without a
metastore service. This lets existing file-based Iceberg tables remain where they are while Polaris
brokers access, policies, and multi-engine connectivity.

{{% alert title="Note" color="primary" %}}
The Iceberg documentation states that the Hadoop catalog "can only be used with HDFS or similar
file systems that support atomic rename" and that "concurrent writes with a Hadoop catalog are not
safe with a local FS or S3". Those constraints apply unchanged when the catalog is reached through
Polaris.
{{% /alert %}}

## Build-time enablement

Unlike Hive and BigQuery federation, the Hadoop factory is part of the default server build:
`runtime/server/build.gradle.kts` always includes `polaris-extensions-federation-hadoop`, so no
Gradle property is needed. Standard Polaris binaries and container images can federate to Hadoop
catalogs as soon as the feature flags below are set.

## Feature configuration

Enable the necessary feature flags in your `application.properties` file (or equivalent
configuration mechanism such as environment variables or a Kubernetes ConfigMap):

```properties
# Allows both REST and HADOOP connection types
polaris.features."SUPPORTED_CATALOG_CONNECTION_TYPES"=["ICEBERG_REST","HADOOP"]

# Allows IMPLICIT authentication, needed for Hadoop federation
polaris.features."SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES"=["OAUTH","IMPLICIT"]

# Enables the federation feature itself
polaris.features."ENABLE_CATALOG_FEDERATION"=true
```

For Kubernetes deployments, add these properties to the ConfigMap mounted into the Polaris container
(typically at `/deployment/config/application.properties`).

## Runtime requirements

- **Warehouse access:** The Polaris server itself lists, reads, and writes the warehouse directory.
  There is no remote service in between, so the file system behind the `warehouse` location must be
  reachable from the Polaris process with permission to do all three.
- **Configuration discovery:** Polaris initializes the `HadoopCatalog` with a default Hadoop
  `Configuration`, which loads `core-site.xml` and `hdfs-site.xml` from the classpath. Provide them
  via `HADOOP_CONF_DIR` or an image layer when the warehouse is on HDFS or needs non-default client
  settings.
- **Authentication:** Hadoop federation only supports `IMPLICIT` authentication, meaning Polaris
  uses the operating-system or Kerberos identity of the running process (no stored secrets). For a
  Kerberized HDFS, ensure the service principal holds a valid keytab/TGT before starting Polaris;
  the [Kerberos setup example]({{% ref "hive-metastore-federation.md#kerberos-setup-example" %}})
  for Hive federation applies here as well.
- **Object storage role:** When the warehouse is on S3, configure
  `polaris.service-identity.<realm>.aws-iam.*` (or the default realm) so the server can assume the
  AWS role referenced by the catalog, and read the warehouse access notes below.

## Warehouse access patterns

The default Polaris build ships the Hadoop client libraries, but not cloud connectors such as
`hadoop-aws`. Polaris also has no HDFS storage type: `allowedLocations` and `default-base-location`
must use a prefix accepted by one of the `FILE`, `S3`, `AZURE` or `GCS` storage types. Together this
leads to the following patterns:

| Pattern                                                                              | Works with the default build       | Notes                                                                                                                                                                                                                                                      |
|--------------------------------------------------------------------------------------|------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Local or network-mounted directory (`file:///...`) with `FILE` storage               | Yes                                | Simplest setup for tests and single-writer deployments. `FILE` storage is intended for testing; it performs no credential vending.                                                                                                                         |
| HDFS warehouse handled by ambient Hadoop config and process identity                 | Only with wildcard locations       | `hdfs://` is not an accepted prefix for any storage type. Use `FILE` storage with `allowedLocations: ["*"]` and `ALLOW_WILDCARD_LOCATION=true`, which disables Polaris location checks for the catalog, so restrict access at the HDFS permission level.   |
| `HadoopFileIO` with `s3a://` warehouses                                              | Only with extra runtime packaging  | `hadoop-aws` and related file system dependencies are not included in the Polaris runtime.                                                                                                                                                                 |
| `S3FileIO` with ambient AWS credentials                                              | Sometimes                          | Set `io-impl=org.apache.iceberg.aws.s3.S3FileIO` in the catalog-level `properties`. Commits still rely on atomic rename, which S3 does not provide, so concurrent writers are unsafe.                                                                       |
| **UNSAFE!** Putting object-store credentials in catalog properties                   | **DO NOT USE / THIS IS UNSAFE**    | Secrets placed in catalog properties are returned to authenticated catalog clients through `/config`. This **exposes passwords, access keys, session tokens, or other secrets as plain text**.                                                             |

## Creating a federated catalog

Use the Management API (or the Python CLI) to create an external catalog whose connection type is
`HADOOP`. The following request registers a catalog whose tables live under
`file:///data/warehouse/analytics`, a directory mounted into the Polaris container:

```bash
curl -X POST https://<polaris-host>/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "type": "EXTERNAL",
        "name": "analytics_hadoop",
        "storageConfigInfo": {
          "storageType": "FILE",
          "allowedLocations": ["file:///data/warehouse/analytics/"]
        },
        "properties": { "default-base-location": "file:///data/warehouse/analytics/" },
        "connectionConfigInfo": {
          "connectionType": "HADOOP",
          "uri": "file:///data/warehouse/analytics/",
          "warehouse": "file:///data/warehouse/analytics/",
          "authenticationParameters": { "authenticationType": "IMPLICIT" }
        }
      }'
```

The same catalog can be created with the CLI:

```bash
polaris catalogs create analytics_hadoop \
  --type external \
  --storage-type file \
  --default-base-location file:///data/warehouse/analytics/ \
  --catalog-connection-type hadoop \
  --catalog-uri file:///data/warehouse/analytics/ \
  --hadoop-warehouse file:///data/warehouse/analytics/ \
  --catalog-authentication-type implicit
```

`warehouse` is the root directory that the `HadoopCatalog` scans for namespaces and tables. `uri`
is required by the connection configuration API but is not used by the Hadoop catalog. Polaris
validates it as a `java.net.URL`, so it must use a scheme the JVM knows (`file`, `http`, `https`);
`hdfs://` and `s3://` values are rejected with `Invalid remote URI`. For a `file://` warehouse,
reuse the warehouse location; for HDFS or S3 warehouses, use a descriptive `http(s)` address such as
the NameNode web UI. `default-base-location` is required; it tells Polaris and Iceberg where to
place new metadata files. For an S3 warehouse, use `"storageType": "S3"` with `roleArn` and
`region` as in the Hive federation example.

Grant catalog roles to principal roles exactly as you would for internal catalogs so engines can
obtain tokens that authorize against the federated metadata.

## Connection properties

`connectionConfigInfo.properties` holds settings for the Hadoop catalog client that Polaris creates
for the federated catalog. Unlike catalog-level `properties`, these values are visible only through
the Management API and are never returned to catalog clients via `/v1/config`, so they are the
right place for non-secret client settings. Only the following keys are forwarded to the Iceberg
`HadoopCatalog`; any other key is stored but ignored:

| Key | Purpose |
|-----|---------|
| `suppress-permission-error` | Skip directories the Polaris process may not read when listing namespaces and tables, instead of failing the whole listing |
| `s3.endpoint`, `s3.path-style-access`, `client.region` | Non-secret FileIO client settings for S3-compatible warehouses |

The `s3.*` and `client.region` keys only take effect when the catalog uses `S3FileIO`. `HadoopCatalog`
defaults to `HadoopFileIO`, which ignores them, so set
`io-impl=org.apache.iceberg.aws.s3.S3FileIO` in the catalog-level `properties` (it is not accepted
as a connection property) when you rely on them.

Connection properties take precedence over catalog-level `properties` when the same key is present.
Credentials must never be placed here; use the Polaris process identity as described above.

## Limitations and operational notes

- **Single identity:** Because only `IMPLICIT` authentication is permitted, Polaris cannot mix
  multiple Hadoop identities in a single deployment (`HadoopFederatedCatalogFactory` rejects other
  auth types). Plan a deployment topology that aligns the Polaris process identity with the
  warehouse file system.
- **Generic tables:** The Hadoop extension exposes Iceberg tables found in the warehouse. Generic
  table federation is not implemented (`HadoopFederatedCatalogFactory#createGenericCatalog` throws
  `UnsupportedOperationException`).
- **Namespace and table discovery:** The Hadoop catalog derives namespaces and tables from the
  directory layout under `warehouse`. A directory whose `metadata` subdirectory contains a
  `*.metadata.json` file is a table; any other directory is a namespace. Tables stored outside this
  layout are not visible through Polaris.
- **Namespace properties:** The Hadoop catalog does not store namespace properties. Creating a
  namespace with properties, or setting and removing them later, fails with
  `UnsupportedOperationException`.

With these constraints satisfied, Polaris can sit in front of a file-based Iceberg warehouse so
that its tables gain OAuth-protected, multi-engine access through the Polaris REST APIs.

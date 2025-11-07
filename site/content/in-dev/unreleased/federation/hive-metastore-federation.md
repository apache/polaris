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
title: Hive Metastore Federation
type: docs
weight: 705
---

Polaris can federate catalog operations to an existing Hive Metastore (HMS). This lets an external
HMS remain the source of truth for table metadata while Polaris brokers access, policies, and
multi-engine connectivity.

## Build-time enablement

The Hive factory is packaged as an optional extension and is not baked into default server builds.
Include it when assembling the runtime or container images by setting the `NonRESTCatalogs` Gradle
property to include `HIVE` (and any other non-REST backends you need):

```bash
./gradlew :polaris-server:assemble :polaris-server:quarkusAppPartsBuild --rerun \
  -DNonRESTCatalogs=HIVE -Dquarkus.container-image.build=true
```

`runtime/server/build.gradle.kts` wires the extension in only when this flag is present, so binaries
built without it will reject Hive federation requests.

## Feature configuration

After building Polaris with Hive support, enable the necessary feature flags in your
`application.properties` file (or equivalent configuration mechanism such as environment variables or
a Kubernetes ConfigMap):

```properties
# Allows both REST and HIVE connection type
polaris.features."SUPPORTED_CATALOG_CONNECTION_TYPES"=["ICEBERG_REST","HIVE"]

# Allows IMPLICIT authentication, needed for Hive federation
polaris.features."SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES"=["OAUTH","IMPLICIT"]

# Enables the federation feature itself
polaris.features."ENABLE_CATALOG_FEDERATION"=true
```

For Kubernetes deployments, add these properties to the ConfigMap mounted into the Polaris container
(typically at `/deployment/config/application.properties`).

## Runtime requirements

- **Metastore connectivity:** Expose the HMS Thrift endpoint (`thrift://host:port`) to the Polaris
  deployment.
- **Configuration discovery:** Iceberg’s `HiveCatalog` loads Hadoop/Hive client settings from the
  classpath. Provide `hive-site.xml` (and `core-site.xml` if needed) via
  `HADOOP_CONF_DIR`/`HIVE_CONF_DIR` or an image layer.
- **Authentication:** Hive federation only supports `IMPLICIT` authentication, meaning Polaris uses
  the operating-system or Kerberos identity of the running process (no stored secrets). Ensure the
  service principal is logged in or holds a valid keytab/TGT before starting Polaris.
- **Object storage role:** Configure `polaris.service-identity.<realm>.aws-iam.*` (or the default
  realm) so the server can assume the AWS role referenced by the catalog. The IAM role must allow
  STS access from the Polaris service identity and grant permissions to the table locations.

### Kerberos setup example

If your Hive Metastore enforces Kerberos, stage the necessary configuration alongside Polaris:

```bash
export KRB5_CONFIG=/etc/polaris/krb5.conf
export HADOOP_CONF_DIR=/etc/polaris/hadoop-conf   # contains hive-site.xml with HMS principal
export HADOOP_OPTS="-Djava.security.auth.login.config=/etc/polaris/jaas.conf"
kinit -kt /etc/polaris/keytabs/polaris.keytab polaris/service@EXAMPLE.COM
```

- `hive-site.xml` must define `hive.metastore.sasl.enabled=true`, the metastore principal, and
  client principal pattern (for example `hive.metastore.client.kerberos.principal=polaris/_HOST@REALM`).
- The JAAS entry (referenced by `java.security.auth.login.config`) should use `useKeyTab=true` and
  point to the same keytab shown above so the Polaris JVM can refresh credentials automatically.
- Keep the keytab readable solely by the Polaris service user; the implicit authenticator consumes
  the TGT at startup and for periodic renewal.

## Creating a federated catalog

Use the Management API (or the Python CLI) to create an external catalog whose connection type is
`HIVE`. The following request registers a catalog that proxies to an HMS running on
`thrift://hms.example.internal:9083`:

```bash
curl -X POST https://<polaris-host>/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "type": "EXTERNAL",
        "name": "analytics_hms",
        "storageConfigInfo": {
          "storageType": "S3",
          "roleArn": "arn:aws:iam::123456789012:role/polaris-warehouse-access",
          "region": "us-east-1"
        },
        "properties": { "default-base-location": "s3://analytics-bucket/warehouse/" },
        "connectionConfigInfo": {
          "connectionType": "HIVE",
          "uri": "thrift://hms.example.internal:9083",
          "warehouse": "s3://analytics-bucket/warehouse/",
          "authenticationParameters": { "authenticationType": "IMPLICIT" }
        }
      }'
```

Grant catalog roles to principal roles exactly as you would for internal catalogs so engines can
obtain tokens that authorize against the federated metadata.

`default-base-location` is required; it tells Polaris and Iceberg where to place new metadata files.
`allowedLocations` is optional—supply it only when you want to restrict writers to a specific set of
prefixes. If your IAM trust policy requires an `externalId` or explicit `userArn`, include those
optional fields in `storageConfigInfo`. Polaris persists them and supplies them when assuming the
role cited by `roleArn` during metadata commits.

## Limitations and operational notes

- **Single identity:** Because only `IMPLICIT` authentication is permitted, Polaris cannot mix
  multiple Hive identities in a single deployment (`HiveFederatedCatalogFactory` rejects other auth
  types). Plan a deployment topology that aligns the Polaris process identity with the target HMS.
- **Generic tables:** The Hive extension exposes Iceberg tables registered in HMS. Generic table
  federation is not implemented (`HiveFederatedCatalogFactory#createGenericCatalog` throws
  `UnsupportedOperationException`).
- **Configuration caching:** Atlas-style catalog failover and multi-HMS routing are not yet handled;
  Polaris initializes one `HiveCatalog` per connection and relies on the underlying Iceberg client
  for retries.

With these constraints satisfied, Polaris can sit in front of an HMS so that Iceberg tables managed
there gain OAuth-protected, multi-engine access through the Polaris REST APIs.

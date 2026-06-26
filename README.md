<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Polaris

Apache Polaris&trade; is an open-source, fully-featured catalog for Apache Iceberg&trade;. It implements Iceberg's
[REST API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml),
enabling seamless multi-engine interoperability across a wide range of platforms, including Apache Doris&trade;, Apache Flink&reg;,
Apache Spark&trade;, Dremio&reg; OSS, StarRocks, and Trino.

Documentation is available at https://polaris.apache.org. The REST OpenAPI specifications can be browsed online:
[Polaris Management API](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/polaris/refs/heads/main/spec/polaris-management-service.yml)
and [Polaris Catalog API](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/polaris/refs/heads/main/spec/generated/bundled-polaris-catalog-service.yaml).

For a high-level, auto-generated tour of the codebase's modules and relationships, see the
[Code Wiki for Apache Polaris](https://codewiki.google/github.com/apache/polaris).
It is a third-party, auto-generated navigation aid, useful for orientation, but the
source tree remains the authoritative reference.

To get involved, [subscribe to the dev mailing list][dev-list-subscribe] (or browse [the archives](https://lists.apache.org/list.html?dev@polaris.apache.org))
and read the [CONTRIBUTING guide](CONTRIBUTING.md).

[![Slack](https://img.shields.io/badge/chat-on%20Slack-brightgreen.svg?style=for-the-badge)](https://join.slack.com/t/apache-polaris/shared_invite/zt-2y3l3r0fr-VtoW42ltir~nSzCYOrQgfw)
[![Build Status](https://img.shields.io/github/actions/workflow/status/apache/polaris/ci-main.yml?branch=main&label=Main%20CI&logo=Github&style=for-the-badge)](https://github.com/apache/polaris/actions/workflows/ci-main.yml?query=branch%3Amain)

[dev-list-subscribe]: mailto:dev-subscribe@polaris.apache.org

## Overview & Quickstart

- [Overview](https://polaris.apache.org/releases/latest/) of what Polaris is and how it works.
- [Quickstart](https://polaris.apache.org/releases/latest/getting-started/) to set up a Polaris instance locally or on a supported cloud provider.

## Project Structure

The repository is organized into the following groups of Gradle modules. Each leaf is a Gradle subproject; some link to a per-module README.

- **Core**
  - [`polaris-core`](./polaris-core/README.md) - entity definitions and core business logic
- **[API](./api/README.md)** - generated from the OpenAPI specifications:
  - `polaris-api-management-model`, `polaris-api-management-service` - Polaris Management API
  - `polaris-api-iceberg-service` - Iceberg REST service
  - `polaris-api-catalog-service` - Polaris Catalog API
- **Runtime**
  - [`polaris-server`](./runtime/server/README.md) - Quarkus-based server
  - [`polaris-admin`](./runtime/admin/README.md) - admin tool, mainly for bootstrapping persistence
  - [`polaris-runtime-service`](./runtime/service/README.md) - the Polaris service package
  - [`polaris-runtime-defaults`](./runtime/defaults/README.md) - default runtime configuration
  - [`polaris-distribution`](./runtime/distribution/README.md) - distribution packaging
  - `polaris-runtime-common`, `polaris-runtime-test-common` - shared runtime and test utilities
  - `polaris-runtime-spark-tests` - integration tests for the Spark plugin
- **Persistence**
  - `polaris-relational-jdbc` - JDBC implementation of `BasePersistence`
- **Extensions**
  - [`polaris-extensions-federation-hive`](./extensions/federation/hive/README.md), `polaris-extensions-federation-hadoop`, `polaris-extensions-federation-bigquery` - catalog federation
  - `polaris-extensions-auth-opa`, `polaris-extensions-auth-ranger` (plus `*-tests`) - external authorization
- **Tooling & build support**
  - `polaris-bom`, `polaris-build-logic`, `polaris-version` - BOM, shared build logic, versioning
  - `polaris-immutables`, `polaris-misc-types`, `polaris-container-spec-helper` - shared utilities
  - `polaris-minio-testcontainer`, `polaris-rustfs-testcontainer`, `polaris-hms-testcontainer` - test containers
  - `polaris-config-docs-{annotations,generator,site}` - reference-doc generation
  - `aggregated-license-report` - aggregated license report
- **Tests**
  - [`polaris-tests`](./integration-tests/README.md) - normative integration tests, reusable downstream

Other top-level directories:

- [`spec/`](./spec/README.md) - OpenAPI specifications
- [`client/python/`](./client/python/README.md) - Python client
- [`plugins/spark/`](./plugins/spark/README.md) - Polaris Spark plugin
- [`regtests/`](./regtests/README.md) - regression tests
- [`helm/`](./helm) - Helm charts
- [`site/`](./site/README.md) - documentation site
- [`codestyle/`](./codestyle), [`gradle/`](./gradle), [`server-templates/`](./server-templates) - build, style, and codegen support

Additional tooling lives in the separate [Polaris-Tools](https://github.com/apache/polaris-tools) repository.

## Building and Running

Apache Polaris is built using Gradle with Java 21+ and Docker 27+.

- `./gradlew build` - To build and run tests. Make sure Docker is running, as the integration tests depend on it.
- `./gradlew assemble` - To skip tests.
- `./gradlew check` - To run all checks, including unit tests and integration tests.
  To run all checks except integration tests, use `./gradlew check -PnoIntegrationTests`.
- `./gradlew run` - To run the Polaris server locally; the server is reachable at localhost:8181. This is also suitable for running regression tests, or for connecting with Spark. Set your own credentials by specifying system property `./gradlew run -Dpolaris.bootstrap.credentials=POLARIS,root,secret` where:
  - `POLARIS` is the realm
  - `root` is the CLIENT_ID
  - `secret` is the CLIENT_SECRET
  - If credentials are not set, it will use preset credentials `POLARIS,root,s3cr3t`
- `./regtests/run_spark_sql.sh` - To connect from Spark SQL. Here are some example commands to run in the Spark SQL shell:
```sql
create database db1;
show databases;
create table db1.table1 (id int, name string);
insert into db1.table1 values (1, 'a');
select * from db1.table1;
```
- `env POLARIS_HOST=localhost ./regtests/run.sh` - To run regression tests locally, see more options [here](./regtests/README.md).

## Makefile Convenience Commands

- `make build-server`, `make build-admin` build components and container images
- `make minikube-start-cluster`, `make minikube-cleanup` manage a local minikube cluster
- `make helm-doc-generate`, `make helm-unittest` work with the Helm charts
- `make client-lint`, `make client-regenerate` work with the Python client
- `make install-dependencies-brew` install developer prerequisites on macOS

### Running in Docker

Build the image locally:

```bash
./gradlew \
  :polaris-server:assemble \
  :polaris-server:quarkusAppPartsBuild --rerun \
  -Dquarkus.container-image.build=true
```

Run the published image:

```bash
docker run -p 8181:8181 -p 8182:8182 apache/polaris:latest
```

The repository also ships docker-compose examples for various configurations. See the
[Quickstart](https://polaris.apache.org/releases/latest/getting-started/quick-start/) for details.

### Running in Kubernetes

See [`helm/polaris/README.md`](helm/polaris/README.md).

### Configuring Polaris

Servers can be configured in several ways: see the [Configuration Guide](https://polaris.apache.org/releases/latest/configuration/).
Default values live in [`runtime/defaults/src/main/resources/application.properties`](./runtime/defaults/src/main/resources/application.properties).

### Building the docs

Docs use [Hugo](https://gohugo.io/) with the [Docsy](https://www.docsy.dev/docs/) theme. To preview locally:

```bash
site/bin/run-hugo-in-docker.sh
```

See [`site/README.md`](site/README.md) for more.

### Develocity build scans

Build scans for `apache/polaris` branch and tag CI runs are published to the ASF Develocity instance at
[develocity.apache.org](https://develocity.apache.org/scans?search.rootProjectNames=polaris) when the
`DEVELOCITY_ACCESS_KEY` org secret is available. Pull-request CI publishes to Gradle's public Develocity
instance instead.

Local builds publish a scan only when invoked with `--scan`, and only after you accept Gradle's
[terms of service](https://gradle.com/terms-of-service). Forks and other CI environments can opt in by setting
`GRADLE_TOS_ACCEPTED=true`. Optional overrides: `DEVELOCITY_PROJECT_ID`, `DEVELOCITY_SERVER`, and a
`DEVELOCITY_ACCESS_KEY` GitHub secret if you self-host Develocity.

## License

Apache Polaris is under the Apache License Version 2.0. See the [LICENSE](LICENSE).

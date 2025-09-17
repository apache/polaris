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

# Apache Polaris (incubating)

Apache Polaris&trade; is an open-source, fully-featured catalog for Apache Iceberg&trade;. It implements Iceberg's 
[REST API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml),
enabling seamless multi-engine interoperability across a wide range of platforms, including Apache Doris™, Apache Flink®,
Apache Spark™, Dremio® OSS, StarRocks, and Trino. 

Documentation is available at https://polaris.apache.org. The REST OpenAPI specifications are available here:
[Polaris management API doc](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/polaris/refs/heads/main/spec/polaris-management-service.yml)
and [Polaris Catalog API doc](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/polaris/refs/heads/main/spec/generated/bundled-polaris-catalog-service.yaml).

[Subscribe to the dev mailing list][dev-list-subscribe] to join discussions via email or browse [the archives](https://lists.apache.org/list.html?dev@polaris.apache.org). Check out the [CONTRIBUTING guide](CONTRIBUTING.md)
for contribution guidelines.

[![Slack](https://img.shields.io/badge/chat-on%20Slack-brightgreen.svg?style=for-the-badge)](https://join.slack.com/t/apache-polaris/shared_invite/zt-2y3l3r0fr-VtoW42ltir~nSzCYOrQgfw)
[![Build Status](https://img.shields.io/github/actions/workflow/status/apache/polaris/gradle.yml?branch=main&label=Main%20CI&logo=Github&style=for-the-badge)](https://github.com/apache/polaris/actions/workflows/gradle.yml?query=branch%3Amain)

[dev-list-subscribe]: mailto:dev-subscribe@polaris.apache.org

## Polaris Overview
Click [here](https://polaris.apache.org/in-dev/unreleased/) for a quick overview of Polaris.

## Quickstart
Click [here](https://polaris.apache.org/in-dev/unreleased/getting-started/install-dependencies/) for the quickstart experience, which will help you set up a Polaris instance locally or on any supported cloud provider.

## Building and Running 

Apache Polaris is organized into the following modules:

- `polaris-core` - The main Polaris entity definitions and core business logic
- API modules (implementing the Iceberg REST API and Polaris management API):
  - `polaris-api-management-model` - The Polaris management model
  - `polaris-api-management-service` - The Polaris management service
  - `polaris-api-iceberg-service` - The Iceberg REST service
- Runtime modules:
  - `polaris-runtime-service` - The runtime components of the Polaris server
  - `polaris-runtime-defaults` - The runtime configuration defaults
  - `polaris-server` - The Polaris server
  - `polaris-admin` - The Polaris admin & maintenance tool
- Persistence modules:
  - `polaris-eclipselink` - The Eclipselink implementation of the MetaStoreManager interface
  - `polaris-relational-jdbc` - The JDBC implementation of BasePersistence to be used via AtomicMetaStoreManager
 
Apache Polaris is built using Gradle with Java 21+ and Docker 27+.

- `./gradlew build` - To build and run tests. Make sure Docker is running, as the integration tests depend on it.
- `./gradlew assemble` - To skip tests.
- `./gradlew check` - To run all checks, including unit tests and integration tests.
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

To streamline the developer experience, especially for common setup and build tasks, a root-level Makefile is available. This Makefile acts as a convenient wrapper around various Gradle commands and other tooling, simplifying interactions. While Gradle remains the primary build system, the Makefile provides concise shortcuts for frequent operations like:
  - Building Polaris components: e.g., `make build-server, make build-admin`
  - Managing development clusters: e.g., `make minikube-start-cluster, make minikube-cleanup`
  - Automating Helm tasks: e.g., `make helm-doc-generate, make helm-unittest`
  - Handling dependencies: e.g., `make install-dependencies-brew`
  - Managing client operations: e.g., `make client-lint, make client-regenerate`

To see available commands:
```bash
make help
```

For example, to build the Polaris server and its container image, you can simply run:
```bash
make build-server
```

### More build and run options

#### Running in Docker

- To build the image locally:
  ```bash
  ./gradlew \
    :polaris-server:assemble \
    :polaris-server:quarkusAppPartsBuild --rerun \
    -Dquarkus.container-image.build=true
  ```
- `docker run -p 8181:8181 -p 8182:8182 apache/polaris:latest` - To run the image.

The Polaris codebase contains some docker compose examples to quickly get started with Polaris,
using different configurations. Check the `./getting-started` directory for more information.

#### Running in Kubernetes

- See [README in `helm/polaris`](helm/polaris/README.md) for more information.

#### Configuring Polaris

Polaris Servers can be configured using a variety of ways.
Please see the [Configuration Guide](site/content/in-dev/unreleased/configuration.md)
for more information.

Default configuration values can be found in `runtime/defaults/src/main/resources/application.properties`.

#### Building docs

- Docs are generated using [Hugo](https://gohugo.io/) using the [Docsy](https://www.docsy.dev/docs/) theme.
- To view the site locally, run
  ```bash
  site/bin/run-hugo-in-docker.sh
  ```
- See [README in `site/`](site/README.md) for more information.

#### Publishing Build Scans to develocity.apache.org

Build scans of CI builds from a branch or tag in the `apache/polaris` repository on GitHub publish build scans
to the ASF Develocity instance at
[develocity.apache.org](https://develocity.apache.org/scans?search.rootProjectNames=polaris), if the workflow runs have access to the Apache organization-level secret 
`DEVELOCITY_ACCESS_KEY`.

Build scans of local developer builds publish build scans only if the Gradle command line option `--scan` is used.
Those build scans are published to Gradle's public Develocity instance (see advanced configuration options below).
Note that build scans on Gradle's public Develocity instance are publicly accessible to anyone.
You have to accept Gradle's terms of service to publish to the Gradle's public Develocity instance.

CI builds originating from pull requests against the `apache/polaris` GitHub repository are published to Gradle's
_public_ Develocity instance. 

Other CI build scans do only publish build scans to the Gradle's _public_ Develocity instance, if the environment
variable `GRADLE_TOS_ACCEPTED` is set to `true`.
By setting this variable you agree to the [Gradle's terms of service](https://gradle.com/terms-of-service), because
accepting these ToS is your personal decision. 
You can configure this environment variable for your GitHub repository in the GitHub repository settings under
`Secrets` > `Secrets and variables` > `Actions` > choose the `Variables` tab > `New repository variable`. 

Advanced configuration options for publishing build scans (only local and non-`apache/polaris` repository CI):
* The project ID published with the build scan can be specified using the environment variable `DEVELOCITY_PROJECT_ID`.
  The project ID defaults to the GitHub repository owner/name, for example `octocat/polaris`.
* The Develocity server can be specified using the environment variable `DEVELOCITY_SERVER` if build scans should be
  published to another than Gradle's public Develocity instance.
* If you have to publish build scans to your own Develocity instance, you can configure the access key using a
  GitHub secret named `DEVELOCITY_ACCESS_KEY`.

## License

Apache Polaris is under the Apache License Version 2.0. See the [LICENSE](LICENSE).

## ASF Incubator disclaimer

Apache Polaris&trade; is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.
 
<sub>Apache&reg;, Apache Polaris&trade;, Apache Iceberg&trade;, Apache Spark&trade; are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.</sub>

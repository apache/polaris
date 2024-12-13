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
Apache Spark™, StarRocks, and Trino.

Documentation is available at https://polaris.apache.org, including
[Polaris management API doc](https://polaris.apache.org/index.html#tag/polaris-management-service_other)
and [Apache Iceberg REST API doc](https://polaris.apache.org/index.html#tag/Configuration-API).

[Subscribe to the dev mailing list][dev-list-subscribe] to join discussions via email or browse [the archives](https://lists.apache.org/list.html?dev@polaris.apache.org). Check out the [CONTRIBUTING guide](CONTRIBUTING.md)
for contribution guidelines.

[![Zulip](https://img.shields.io/badge/Zulip-Chat-blue?color=3d4db3&logo=zulip&style=for-the-badge&logoColor=white)](https://polaris-catalog.zulipchat.com/)
[![Build Status](https://img.shields.io/github/actions/workflow/status/apache/polaris/gradle.yml?branch=main&label=Main%20CI&logo=Github&style=for-the-badge)](https://github.com/apache/polaris/actions/workflows/gradle.yml?query=branch%3Amain)

[dev-list-subscribe]: mailto:dev-subscribe@polaris.apache.org

## Building and Running

Apache Polaris is organized into the following modules:
- `polaris-core` - The main Polaris entity definitions and core business logic
- `polaris-server` - The Polaris REST API server
- `polaris-eclipselink` - The Eclipselink implementation of the MetaStoreManager interface

Apache Polaris is built using Gradle with Java 21+ and Docker 27+.
- `./gradlew build` - To build and run tests. Make sure Docker is running, as the integration tests depend on it.
- `./gradlew assemble` - To skip tests.
- `./gradlew test` - To run unit tests and integration tests.
- `./gradlew runApp` - To run the Polaris server locally on localhost:8181.
- `./regtests/run_spark_sql.sh` - To connect from Spark SQL. Here are some example commands to run in the Spark SQL shell:
```sql
create database db1;
show databases;
create table db1.table1 (id int, name string);
insert into db1.table1 values (1, 'a');
select * from db1.table1;
```

Apache Polaris supports the following optional build options:
- `-PeclipseLink=true` – Enables the EclipseLink extension.
- `-PeclipseLinkDeps=[groupId]:[artifactId]:[version],...` – Specifies one or more additional dependencies for EclipseLink (e.g., JDBC drivers) separated by commas.

### More build and run options
Running in Docker
- `docker build -t localhost:5001/polaris:latest .` - To build the image.
  - Optional build options:
    - `docker build -t localhost:5001/polaris:latest --build-arg ECLIPSELINK=true .` - Enables the EclipseLink extension.
    - `docker build -t localhost:5001/polaris:latest --build-arg ECLIPSELINK=true --build-arg ECLIPSELINK_DEPS=[groupId]:[artifactId]:[version],... .` – Enables the EclipseLink extension with one or more additional dependencies for EclipseLink (e.g. JDBC drivers) separated by commas.
- `docker run -p 8181:8181 localhost:5001/polaris:latest` - To run the image in standalone mode.

Running in Kubernetes
- `./run.sh` - To run Polaris as a mini-deployment locally. This will create one pod that bind itself to ports `8181` and `8182`.
  - Optional run options:
    - `./run.sh -b "ECLIPSELINK=true"` - Enables the EclipseLink extension.
    - `./run.sh -b "ECLIPSELINK=true;ECLIPSELINK_DEPS=[groupId]:[artifactId]:[version],..."` – Enables the EclipseLink extension with one or more additional dependencies for EclipseLink (e.g. JDBC drivers) separated by commas.
- `kubectl port-forward svc/polaris-service -n polaris 8181:8181 8182:8182` - To create secure connections between a local machine and a pod within the cluster for both service and metrics endpoints.
  - Currently supported metrics endpoints:
    - localhost:8182/metrics
    - localhost:8182/healthcheck
- `kubectl get pods -n polaris` - To check the status of the pods.
- `kubectl get deployment -n polaris` - To check the status of the deployment.
- `kubectl describe deployment polaris-deployment -n polaris` - To troubleshoot if things aren't working as expected.

Running regression tests
- `./regtests/run.sh` - To run regression tests on localhost (ensure Polaris server start first in another terminal on localhost).
- `docker compose up --build --exit-code-from regtest` - To run regression tests in a Docker environment.

Building docs
- Docs are generated using [Hugo](https://gohugo.io/) using the [Docsy](https://www.docsy.dev/docs/) theme.
- To view the site locally, run
  ```bash
  site/bin/run-hugo-in-docker.sh
  ```
- See [README in `site/`](site/README.md) for more information.

## License

Apache Polaris is under the Apache License Version 2.0. See the [LICENSE](LICENSE).

## ASF Incubator disclaimer

Apache Polaris&trade; is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.

<sub>Apache&reg;, Apache Polaris&trade;, Apache Iceberg&trade;, Apache Spark&trade; are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.</sub>

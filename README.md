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
- API modules (implementing the Iceberg REST API and Polaris management API):
  - `polaris-api-management-model` - The Polaris management model
  - `polaris-api-management-service` - The Polaris management service
  - `polaris-api-iceberg-service` - The Iceberg REST service
- Service modules:
  - `polaris-service-common` - The main components of the Polaris server
- Quarkus runtime modules:
  - `polaris-quarkus-service` - The Quarkus-specific components of the Polaris server
  - `polaris-quarkus-server` - The Polaris server runtime
  - `polaris-quarkus-admin-tool` - The Polaris admin & maintenance tool
- Persistence modules
  - `polaris-jpa-model` - The JPA entity definitions
  - `polaris-eclipselink` - The Eclipselink implementation of the MetaStoreManager interface
 
Apache Polaris is built using Gradle with Java 21+ and Docker 27+.

- `./gradlew build` - To build and run tests. Make sure Docker is running, as the integration tests depend on it.
- `./gradlew assemble` - To skip tests.
- `./gradlew test` - To run unit tests and integration tests.

For local development, you can run the following commands:

- `./gradlew polarisServerRun` - To run the Polaris server
  locally, with profile `prod`; the server is reachable at localhost:8181.
- `./gradlew polarisServerDev` - To run the Polaris server
  locally, in [Dev mode](https://quarkus.io/guides/dev-mode-differences). In dev mode, 
  Polaris uses the `test` Authenticator and `test` TokenBroker; this configuration is suitable for 
  running regressions tests, or for connecting with Spark.
- `./regtests/run_spark_sql.sh` - To connect from Spark SQL. Here are some example commands to run in the Spark SQL shell:
```sql
create database db1;
show databases;
create table db1.table1 (id int, name string);
insert into db1.table1 values (1, 'a');
select * from db1.table1;
```

### More build and run options

#### Running in Docker

Please note: there are no official Docker images for Apache Polaris yet. For now, you can build the
Docker images locally.

To build the Polaris server Docker image locally:

```shell
./gradlew clean :polaris-quarkus-server:assemble -Dquarkus.container-image.build=true
```

To run the Polaris server Docker image:

```shell
docker run -p 8181:8181 -p 8182:8182 apache/polaris:latest
```

#### Running in Kubernetes

- `./run.sh` - To run Polaris as a mini-deployment locally. This will create a Kind cluster, 
  then deploy one pod and one service. The service is available on ports `8181` and `8182`.
- `kubectl port-forward svc/polaris-service -n polaris 8181:8181 8182:8182` - To create secure 
  connections between a local machine and a pod within the cluster for both service and metrics 
  endpoints.
  - Currently supported metrics and health endpoints:
    - http://localhost:8182/q/metrics
    - http://localhost:8182/q/health
- `kubectl get pods -n polaris` - To check the status of the pods.
- `kubectl get deployment -n polaris` - To check the status of the deployment.
- `kubectl describe deployment polaris-deployment -n polaris` - To troubleshoot if things aren't working as expected.

#### Running regression tests

Regression tests can be run in a local environment or in a Docker environment.

To run regression tests locally, you need to have a Polaris server running locally, with the 
`test` Authenticator enabled. You can do this by running Polaris in Quarkus Dev mode, as explained
above:

```shell
./gradlew polarisServerDev
```

Then, you can run the regression tests using the following command:

```shell
./regtests/run.sh
```

To run regression tests in a Docker environment, you can use the following command:

```shell
docker compose -f regtests/docker-compose.yml up --build --exit-code-from regtest
```

The above command will by default run Polaris with the Docker image `apache/polaris:latest`; if you
want to use a different image, you can modify the `docker-compose.yaml` file prior to running it;
alternatively, you can use the following commands to override the image:

```shell
cat <<EOF > regtests/docker-compose.override.yml
services: { polaris: { image: localhost:5001/apache/polaris:latest } }
EOF
docker compose -f regtests/docker-compose.yml up --build --exit-code-from regtest
```

#### Building docs

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

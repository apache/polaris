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

# End-to-end regression tests

regtests provides basic end-to-end tests for spark_sql using spark client jars.

Regression tests are either run in Docker, using docker-compose to orchestrate the tests, or
locally.

**NOTE** regtests are supposed to be a light-weight testing to ensure jars can be used to start
spark and run basic SQL commands. Please use integration for detailed testing.

## Prerequisites

It is recommended to clean the `regtests/output` directory before running tests. This can be done by
running:

```shell
rm -rf ./plugins/spark/v3.5/regtests/output && mkdir -p ./plugins/spark/v3.5/regtests/output && chmod -R 777 ./plugins/spark/v3.5/regtests/output
```

## Run Tests With Docker Compose

Tests can be run with docker-compose using the provided `./plugins/spark/v3.5/regtests/docker-compose.yml` file, as
follows:

```shell
./gradlew assemble publishToMavenLocal
./gradlew \
  :polaris-server:assemble \
  :polaris-server:quarkusAppPartsBuild --rerun \
  -Dquarkus.container-image.build=true
docker compose -f ./plugins/spark/v3.5/regtests/docker-compose.yml up --build --exit-code-from regtest
```

In this setup, a Polaris container will be started in a docker-compose group, using the image
previously built by the Gradle build. Then another container, including a Spark SQL shell, will run
the tests. The exit code will be the same as the exit code of the Spark container. 
**NOTE** Docker compose only support testing with scala 2.12, because no scala 2.13 image is available
for spark 3.5. Scala 2.13 will be supported for Spark 4.0.

This is the flow used in CI and should be done locally before pushing to GitHub to ensure that no
environmental factors contribute to the outcome of the tests.

**Important**: if you are also using minikube, for example to test the Helm chart, you may need to
_unset_ the Docker environment that was pointing to the Minikube Docker daemon, otherwise the image
will be built by the Minikube Docker daemon and will not be available to the local Docker daemon.
This can be done by running, _before_ building the image and running the tests:

```shell
eval $(minikube -p minikube docker-env --unset)
```

## Run Tests Locally

Regression tests can be run locally as well, using the test harness. For local testing, both
Scala 2.12 and Scala 2.13 are supported.

To run regression tests locally, run the following:
- `./gradlew assemble publishToMavenLocal` -- build the Polaris project and Spark Client jars. Publish the binary to local maven repo.
- `./gradlew run` -- start a Polaris server on localhost:8181.
- `env POLARIS_HOST=localhost ./plugins/spark/v3.5/regtests/run.sh` -- run regtests.

Note: the regression tests expect Polaris to run with certain options, e.g. with support for `FILE`
storage, default realm `POLARIS` and root credentials `root:secret`; if you run the above command,
this will be the case. If you run Polaris in a different way, make sure that Polaris is configured
appropriately.

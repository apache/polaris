<!--

 Copyright (c) 2024 Snowflake Computing Inc.
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

-->

![Polaris Catalog Header](docs/img/logos/Polaris-Catalog-BLOG-symmetrical-subhead.png)

Polaris is an open-source, fully-featured catalog for Apache Iceberg™. It implements Iceberg's 
[REST API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml),
enabling seamless multi-engine interoperability across a wide range of platforms, including Apache Doris™, Apache Flink®,
Apache Spark™, StarRocks, and Trino.

Documentation is available at https://polaris.io, including
[Polaris management API doc](https://polaris.io/index.html#tag/polaris-management-service_other)
and [Apache Iceberg REST API doc](https://polaris.io/index.html#tag/Configuration-API).


## Status
Polaris Catalog is open source under an Apache 2.0 license.

- ⭐ Star this repo if you’d like to bookmark and come back to it! 
- 📖 Read the <a href="https://www.snowflake.com/blog/polaris-catalog-open-source/" target="_blank">announcement blog post<a/> for more details!

## Building and Running 

Polaris is organized into the following modules:
- `polaris-core` - The main Polaris entity definitions and core business logic
- `polaris-server` - The Polaris REST API server
- `polaris-eclipselink` - The Eclipselink implementation of the MetaStoreManager interface
 
Polaris is built using Gradle with Java 21+ and Docker 27+.
- `./gradlew build` - To build and run tests. Make sure Docker is running, as the integration tests depend on it.
- `./gradlew assemble` - To skip tests.
- `./gradlew test` - To run unit tests and integration tests.
- `./gradlew runApp` - To run the Polaris server locally on localhost:8181. 
  - The server starts with the in-memory mode, and it prints the auto-generated credentials to STDOUT in a message like this `realm: default-realm root principal credentials: <id>:<secret>`
  - These credentials can be used as "Client ID" and "Client Secret" in OAuth2 requests (e.g. the `curl` command below).
- `./regtests/run.sh` - To run regression tests or end-to-end tests in another terminal.

Running in Docker
- `docker build -t localhost:5001/polaris:latest .` - To build the image.
- `docker run -p 8181:8181 localhost:5001/polaris:latest` - To run the image in standalone mode.
- `docker compose up --build --exit-code-from regtest` - To run regression tests in a Docker environment.

Running in Kubernetes
- `./setup.sh` - To run Polaris as a mini-deployment locally. This will create one pod that bind itself to ports `8181` and `8182`.
- `kubectl port-forward svc/polaris-service -n polaris 8181:8181 8182:8182` - To create secure connections between a local machine and a pod within the cluster for both service and metrics endpoints.
  - Currrently supported metrics endpoints:
    - localhost:8182/metrics
    - localhost:8182/healthcheck
- `kubectl get pods -n polaris` - To check the status of the pods.
- `kubectl get deployment -n polaris` - To check the status of the deployment.
- `kubectl describe deployment polaris-deployment -n polaris` - To troubleshoot if things aren't working as expected.

Building docs
- Docs are generated using [Redocly](https://redocly.com/docs/cli/installation). To regenerate them, run the following
commands from the project root directory.
```bash
docker run -p 8080:80 -v ${PWD}:/spec docker.io/redocly/cli join spec/docs.yaml spec/polaris-management-service.yml spec/rest-catalog-open-api.yaml -o spec/index.yaml --prefix-components-with-info-prop title
docker run -p 8080:80 -v ${PWD}:/spec docker.io/redocly/cli build-docs spec/index.yaml --output=docs/index.html --config=spec/redocly.yaml
```

## Connecting from an Engine
To connect from an engine like Spark, first create a catalog with these steps:
```bash
# Generate a token for the root principal, replacing <CLIENT_ID> and <CLIENT_SECRET> with
# the values from the Polaris server output.
export PRINCIPAL_TOKEN=$(curl -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -d 'grant_type=client_credentials&client_id=<CLIENT_ID>&client_secret=<CLIENT_SECRET>&scope=PRINCIPAL_ROLE:ALL' \
   | jq -r '.access_token')
   
# Create a catalog named `polaris`
curl -i -X POST -H "Authorization: Bearer $PRINCIPAL_TOKEN" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://localhost:8181/api/management/v1/catalogs \
  -d '{"name": "polaris", "id": 100, "type": "INTERNAL", "readOnly": false, "storageConfigInfo": {"storageType": "FILE"}, "properties": {"default-base-location": "file:///tmp/polaris"}}'
```

From here, you can use Spark to create namespaces, tables, etc. More details can be found in the
[Quick Start Guide](https://polaris.io/#section/Quick-Start/Using-Iceberg-and-Polaris).

### Trademark Attribution 

_Apache Iceberg, Iceberg, Apache Spark, Spark, Apache Flink, Flink, Apache Doris, Doris, Apache, the Apache feather logo, the Apache Iceberg project logo, the Apache Spark project logo, the Apache Flink project logo, and the Apache Doris project logo are either registered trademarks or trademarks of The Apache Software Foundation. Copyright © 2024 The Apache Software Foundation._

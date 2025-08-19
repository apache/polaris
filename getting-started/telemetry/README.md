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

# Getting Started with Apache Polaris, Prometheus and Jaeger

This example requires `jq` to be installed on your machine.

1. Build the Polaris image if it's not already present locally:

    ```shell
    ./gradlew \
      :polaris-server:assemble \
      :polaris-server:quarkusAppPartsBuild --rerun \
      -Dquarkus.container-image.build=true
    ```

2. Start the docker compose group by running the following command from the root of the repository:

    ```shell
    export ASSETS_PATH=$(pwd)/getting-started/assets/
    export CLIENT_ID=root
    export CLIENT_SECRET=s3cr3t
    docker compose -f getting-started/telemetry/docker-compose.yml up
    ```

3. To access Polaris from the host machine, first request an access token:

    ```shell
    export POLARIS_TOKEN=$(curl -s http://localhost:8181/api/catalog/v1/oauth/tokens \
       --user root:s3cr3t \
       -d 'grant_type=client_credentials' \
       -d 'scope=PRINCIPAL_ROLE:ALL' | jq -r .access_token)
    ```

4. Then, use the access token in the Authorization header when accessing Polaris; you can also test
   the `Polaris-Request-Id` header; you should see it in all logs and traces:

    ```shell
    curl -v 'http://localhost:8181/api/management/v1/principal-roles' \
      -H "Authorization: Bearer $POLARIS_TOKEN" \
      -H "Polaris-Request-Id: 1234"
    curl -v 'http://localhost:8181/api/catalog/v1/config?warehouse=quickstart_catalog' \
      -H "Authorization: Bearer $POLARIS_TOKEN" \
      -H "Polaris-Request-Id: 5678"
    ```

5. Access the following services:

   - Prometheus UI: browse to http://localhost:9093 to view metrics.
   - Jaeger UI: browse to http://localhost:16686 to view traces.

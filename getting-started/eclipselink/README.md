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

# Getting Started with Apache Polaris, EclipseLink, Postgres and Spark SQL

This example requires `jq` to be installed on your machine.

1. If such an image is not already present, build the Polaris image with support for EclipseLink and
   the Postgres JDBC driver:

    ```shell
    ./gradlew clean :polaris-quarkus-server:assemble :polaris-quarkus-admin:assemble \
       -PeclipseLinkDeps=org.postgresql:postgresql:42.7.4 \
       -Dquarkus.container-image.tag=postgres-latest \
       -Dquarkus.container-image.build=true \
       --no-build-cache
    ```

2. Start the docker compose group by running the following command from the root of the repository:

    ```shell
    docker compose -f getting-started/eclipselink/docker-compose.yml up
    ```

3. Using spark-sql: attach to the running spark-sql container:

    ```shell
    docker attach $(docker ps -q --filter name=spark-sql)
    ```

   You may not see Spark's prompt immediately, type ENTER to see it. A few commands that you can try:

    ```sql
    CREATE NAMESPACE polaris.ns1;
    USE polaris.ns1;
    CREATE TABLE table1 (id int, name string);
    INSERT INTO table1 VALUES (1, 'a');
    SELECT * FROM table1;
    ```

4. To access Polaris from the host machine, first request an access token:

    ```shell
    export POLARIS_TOKEN=$(curl -s http://polaris:8181/api/catalog/v1/oauth/tokens \
       --resolve polaris:8181:127.0.0.1 \
       --user root:s3cr3t \
       -d 'grant_type=client_credentials' \
       -d 'scope=PRINCIPAL_ROLE:ALL' | jq -r .access_token)
    ```

5. Then, use the access token in the Authorization header when accessing Polaris:

    ```shell
    curl -v http://127.0.0.1:8181/api/management/v1/principal-roles -H "Authorization: Bearer $POLARIS_TOKEN"
    curl -v http://127.0.0.1:8181/api/catalog/v1/config?warehouse=polaris_demo -H "Authorization: Bearer $POLARIS_TOKEN"
    ```

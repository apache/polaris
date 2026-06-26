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
linkTitle: "Client: Apache Flink"
title: "Getting Started with Apache Polaris and Apache Flink"
tags:
  - flink
weight: 400
cascade:
    type: guides
menus:
    main:
        parent: Guides
        weight: 400
---

This getting started guide provides a `docker-compose` file to set up [Apache Flink](https://flink.apache.org/) with Apache Polaris. Apache Polaris is configured as an Iceberg REST Catalog in Flink.

1. Build the Polaris server image if it's not already present locally:

    ```shell
    ./gradlew \
       :polaris-server:assemble \
       :polaris-server:quarkusAppPartsBuild --rerun \
       -Dquarkus.container-image.build=true
    ```


2. Start the docker compose group by running the following command from the root of the repository:

    ```shell
    export S3_ENDPOINT=http://rustfs:9000
    docker compose -f site/content/guides/rustfs/docker-compose.yml -f site/content/guides/flink/docker-compose.yml up --build
    ```

<!-- Guide testing: do not exercise the expensive Docker compose setup.
```shell
exit 0
```
-->

3. Open the Flink SQL client inside the running jobmanager container:

    ```shell
    docker exec -it $(docker ps -q --filter name=jobmanager) ./bin/sql-client.sh
    ```

4. Register the Polaris catalog and run a few statements. The S3 endpoint and
   credentials point at the in-network `rustfs` service:

    ```sql
    CREATE CATALOG polaris WITH (
      'type'                 = 'iceberg',
      'catalog-impl'         = 'org.apache.iceberg.rest.RESTCatalog',
      'uri'                  = 'http://polaris:8181/api/catalog',
      'warehouse'            = 'quickstart_catalog',
      'credential'           = 'root:s3cr3t',
      'scope'                = 'PRINCIPAL_ROLE:ALL',
      'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
      's3.endpoint'          = 'http://rustfs:9000',
      's3.path-style-access' = 'true',
      's3.access-key-id'     = 'rustfsadmin',
      's3.secret-access-key' = 'rustfsadmin'
    );

    USE CATALOG polaris;
    CREATE DATABASE ns1;
    USE ns1;

    CREATE TABLE table1 (id BIGINT, name STRING);

    SET 'execution.checkpointing.interval' = '10s';
    INSERT INTO table1 VALUES (1, 'a'), (2, 'b');

    SET 'sql-client.execution.result-mode' = 'tableau';
    SELECT * FROM table1;
    ```

Note: `s3cr3t` is defined as the password for the `root` user in the `rustfs/docker-compose.yml` file.
      `rustfsadmin` is the RustFS access key from the same file.
      Iceberg only makes data visible after a checkpoint commits, so allow ~10s between the INSERT and the SELECT.

## Useful URLs

- Flink Web UI: [http://localhost:8081](http://localhost:8081)
- Polaris REST API: [http://localhost:8181](http://localhost:8181)
- RustFS Console: [http://localhost:9001](http://localhost:9001)

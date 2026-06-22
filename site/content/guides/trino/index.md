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
linkTitle: "Client: Trino"
title: "Getting Started with Apache Polaris and Trino"
tags:
  - trino
weight: 350
cascade:
    type: guides
menus:
    main:
        parent: Guides
        weight: 350
---

This getting started guide provides a `docker-compose` file to set up [Trino](https://trino.io/) with Apache Polaris.
Apache Polaris is configured as an Iceberg REST Catalog, and [RustFS](https://rustfs.com/) provides S3-compatible object storage.

The `docker-compose` file starts the following services:
* `rustfs` — S3-compatible object storage for Iceberg data files
* `polaris` — Apache Polaris running with an in-memory metastore
* `polaris-setup` — bootstraps a catalog, grants, and a namespace in Polaris
* `trino` — Trino configured with an Iceberg connector pointing at Polaris

## Build the Polaris image

If a Polaris image is not already present locally, build one with the following command:

```shell
./gradlew \
   :polaris-server:assemble \
   :polaris-server:quarkusAppPartsBuild --rerun \
   -Dquarkus.container-image.build=true
```

## Run the `docker-compose` file

Start all services from the repo's root directory:

```shell
docker compose -f site/content/guides/trino/docker-compose.yml up
```

Wait until the `trino` service is healthy before proceeding. You can check by watching for the log line `HTTP server started` in the Trino container output.

## Connect to the Trino CLI

Open a Trino CLI session inside the running Trino container:

```shell
docker compose -f site/content/guides/trino/docker-compose.yml exec trino trino
```

## Run SQL queries

The `polaris-setup` service already created a `quickstart_catalog` backed by RustFS. Create a schema, table, and run some queries:

```sql
CREATE SCHEMA polaris.demo;

USE polaris.demo;

CREATE TABLE events (
    event_id   BIGINT,
    event_type VARCHAR,
    user_id    BIGINT,
    created_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (format = 'PARQUET');

INSERT INTO events VALUES
    (1, 'page_view', 101, TIMESTAMP '2024-10-01 10:00:00 UTC'),
    (2, 'click',     102, TIMESTAMP '2024-10-01 11:30:00 UTC'),
    (3, 'purchase',  101, TIMESTAMP '2024-10-02 09:15:00 UTC');

SELECT event_type, count(*) AS cnt
FROM events
GROUP BY event_type
ORDER BY cnt DESC;
```

## Catalog configuration

The Trino Iceberg catalog is defined in `catalog/polaris.properties` and mounted into the Trino container at `/etc/trino/catalog/`. Key properties:

| Property | Value | Description |
|---|---|---|
| `iceberg.catalog.type` | `rest` | Use the Iceberg REST catalog protocol |
| `iceberg.rest-catalog.uri` | `http://polaris:8181/api/catalog` | Polaris REST catalog endpoint |
| `iceberg.rest-catalog.warehouse` | `quickstart_catalog` | Polaris catalog name |
| `iceberg.rest-catalog.security` | `OAUTH2` | OAuth2 client credentials flow |
| `iceberg.rest-catalog.http-headers` | `Polaris-Realm: POLARIS` | Routes requests to the correct Polaris realm |
| `iceberg.rest-catalog.vended-credentials-enabled` | `true` | Use credentials vended by Polaris to access object storage, instead of static S3 credentials |
| `fs.s3.enabled` | `true` | Enable native S3 file system |
| `s3.endpoint` | `http://rustfs:9000` | RustFS S3-compatible endpoint |
| `s3.path-style-access` | `true` | Required for RustFS |

## RustFS endpoints

The catalog configuration uses different endpoints for Polaris and Trino versus external clients.
`endpointInternal` (`http://rustfs:9000`) is used by Polaris when writing Iceberg metadata files.
`endpoint` (`http://localhost:9000`) is the externally reachable address for clients running outside Docker.
Trino configures its S3 endpoint directly in `catalog/polaris.properties` using the internal Docker hostname `rustfs`.

## Useful URLs

- Trino Web UI: [http://localhost:8080](http://localhost:8080)
- Polaris REST API: [http://localhost:8181](http://localhost:8181)
- RustFS Console: [http://localhost:9001](http://localhost:9001)

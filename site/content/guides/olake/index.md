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
linkTitle: "Client: OLake"
title: "Getting Started with Apache Polaris and OLake"
description: "Replicates a PostgreSQL database into Iceberg tables with OLake, using Polaris as the Iceberg REST Catalog and Trino to query the result."
tags:
  - olake
  - cdc
weight: 375
cascade:
    type: guides
menus:
    main:
        parent: Guides
        weight: 375
---

This getting started guide shows how to load data from an operational database into Iceberg tables managed by Apache Polaris.
[OLake](https://github.com/datazip-inc/olake) replicates a PostgreSQL table, including ongoing changes through logical replication (CDC),
and registers the tables in Polaris through the Iceberg REST API. Trino then queries the tables through the same Polaris catalog.

The setup reuses the RustFS and Trino `docker-compose` files and starts the following services:
* `rustfs` — S3-compatible object storage for Iceberg data files
* `polaris` — Apache Polaris running with an in-memory metastore
* `polaris-setup` — bootstraps the `quickstart_catalog` catalog and its grants
* `trino` — Trino configured with an Iceberg connector pointing at Polaris

PostgreSQL and OLake run as plain containers on the same Docker network.

## Build the Polaris image

If a Polaris image is not already present locally, build one with the following command:

```shell
./gradlew \
   :polaris-server:assemble \
   :polaris-server:quarkusAppPartsBuild --rerun \
   -Dquarkus.container-image.build=true
```

## Run the `docker-compose` files

Start all services from the repo's root directory. The project name `polaris-olake` fixes the Docker network
name to `polaris-olake_default`, which the PostgreSQL and OLake containers join below:

```shell
export S3_ENDPOINT=http://rustfs:9000
docker compose -p polaris-olake -f site/content/guides/rustfs/docker-compose.yml -f site/content/guides/trino/docker-compose.yml up -d
```

Wait until the `trino` service is healthy before proceeding.

<!-- Guide testing: do not exercise the expensive Docker compose setup.
```shell
exit 0
```
-->

## Start PostgreSQL

Start PostgreSQL with logical replication enabled, which OLake needs for change data capture (CDC).
The [OLake PostgreSQL connector documentation](https://olake.io/docs/connectors/postgres) lists all CDC requirements:

```shell
docker run -d --name olake-postgres --network polaris-olake_default -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=shop postgres:17 -c wal_level=logical
```

Wait until PostgreSQL accepts connections:

```shell
until docker exec olake-postgres pg_isready -h localhost -U postgres -d shop; do sleep 1; done
```

Create a sample table, plus the publication and replication slot that OLake reads changes from:

```shell
docker exec -i olake-postgres psql -U postgres -d shop <<'EOF'
CREATE TABLE orders (
    order_id    BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    status      VARCHAR(32) NOT NULL,
    amount      NUMERIC(10, 2) NOT NULL
);
INSERT INTO orders VALUES (1, 101, 'placed', 25.00), (2, 102, 'placed', 42.50), (3, 101, 'shipped', 13.99);
CREATE PUBLICATION olake_publication FOR ALL TABLES;
SELECT pg_create_logical_replication_slot('olake_slot', 'pgoutput');
EOF
```

## Configure OLake

Create a working directory for the OLake configuration and the files OLake generates:

```shell
mkdir -p olake-config
```

The source configuration points at PostgreSQL and enables CDC through the replication slot and publication:

```shell
cat > olake-config/source.json <<'EOF'
{
  "host": "olake-postgres",
  "port": 5432,
  "database": "shop",
  "username": "postgres",
  "password": "postgres",
  "ssl": { "mode": "disable" },
  "update_method": {
    "replication_slot": "olake_slot",
    "publication": "olake_publication",
    "initial_wait_time": 120
  },
  "max_threads": 4
}
EOF
```

The destination configuration points at the `quickstart_catalog` catalog in Polaris. All options are described in the
[OLake Polaris catalog documentation](https://olake.io/docs/writers/iceberg/catalog/rest?rest-catalog=polaris):

```shell
cat > olake-config/destination.json <<'EOF'
{
  "type": "ICEBERG",
  "writer": {
    "catalog_type": "polaris",
    "rest_catalog_url": "http://polaris:8181/api/catalog",
    "iceberg_s3_path": "quickstart_catalog",
    "rest_auth_type": "oauth2",
    "oauth2_uri": "http://polaris:8181/api/catalog/v1/oauth/tokens",
    "credential": "root:s3cr3t",
    "scope": "PRINCIPAL_ROLE:ALL",
    "rest_access_delegation": "vended-credentials"
  }
}
EOF
```

| Property | Value | Description |
|---|---|---|
| `catalog_type` | `polaris` | Use the Iceberg REST catalog protocol with Polaris defaults |
| `rest_catalog_url` | `http://polaris:8181/api/catalog` | Polaris REST catalog endpoint |
| `iceberg_s3_path` | `quickstart_catalog` | Polaris catalog name, sent as the REST `warehouse` |
| `oauth2_uri` | `http://polaris:8181/api/catalog/v1/oauth/tokens` | Polaris token endpoint |
| `credential` | `root:s3cr3t` | OAuth2 client credentials, as `client_id:client_secret` |
| `scope` | `PRINCIPAL_ROLE:ALL` | Activates all principal roles of the client |
| `rest_access_delegation` | `vended-credentials` | Use credentials vended by Polaris to access object storage, instead of static S3 credentials |

Note: `s3cr3t` is defined as the password for the `root` user in the `rustfs/docker-compose.yml` file.
      Outside a local setup, create a dedicated Polaris principal for OLake, granted a catalog role with
      `CATALOG_MANAGE_CONTENT` on the target catalog, instead of using `root`.

## Discover the source tables

OLake runs as a Docker container (see the [OLake Docker CLI documentation](https://olake.io/docs/install/docker-cli)).
The `discover` command writes the tables it finds to `olake-config/available_streams.json` and `olake-config/selected_streams.json`:

```shell
docker run --rm --network polaris-olake_default -v "$PWD/olake-config:/mnt/config" olakego/source-postgres:v1.0.2 discover --config /mnt/config/source.json
```

Because `source.json` defines an `update_method`, the `orders` stream is selected with `"sync_mode": "cdc"`.

## Sync into Polaris

Run the sync. OLake first copies the existing rows, then applies the changes recorded in the replication slot:

```shell
docker run --rm --network polaris-olake_default -v "$PWD/olake-config:/mnt/config" olakego/source-postgres:v1.0.2 sync --config /mnt/config/source.json --available-streams /mnt/config/available_streams.json --selected-streams /mnt/config/selected_streams.json --destination /mnt/config/destination.json
```

## Query the tables with Trino

Open a Trino CLI session inside the running Trino container:

```shell
docker exec -it $(docker ps -q --filter name=trino) trino
```

OLake writes the table to the `postgres_shop_public` namespace, derived from the source type, database and schema.
It also creates a `test_olake` namespace while checking that it can write to the catalog.

```sql
SHOW SCHEMAS FROM polaris;

SELECT order_id, customer_id, status, amount
FROM polaris.postgres_shop_public.orders
ORDER BY order_id;
```

## Replicate a change

Update a row in PostgreSQL:

```shell
docker exec olake-postgres psql -U postgres -d shop -c "UPDATE orders SET status = 'shipped' WHERE order_id = 2;"
```

Run the sync again, passing the state file written by the first run. OLake resumes from the replication slot
and commits the update to the Iceberg table:

```shell
docker run --rm --network polaris-olake_default -v "$PWD/olake-config:/mnt/config" olakego/source-postgres:v1.0.2 sync --config /mnt/config/source.json --available-streams /mnt/config/available_streams.json --selected-streams /mnt/config/selected_streams.json --destination /mnt/config/destination.json --state /mnt/config/state.json
```

The same `SELECT` in Trino now shows order `2` as `shipped`.

## Clean up

```shell
docker rm -f olake-postgres
docker compose -p polaris-olake -f site/content/guides/rustfs/docker-compose.yml -f site/content/guides/trino/docker-compose.yml down
rm -rf olake-config
```

## Useful URLs

- Trino Web UI: [http://localhost:8080](http://localhost:8080)
- Polaris REST API: [http://localhost:8181](http://localhost:8181)
- RustFS Console: [http://localhost:9001](http://localhost:9001)
- OLake Polaris catalog documentation: [https://olake.io/docs/writers/iceberg/catalog/rest?rest-catalog=polaris](https://olake.io/docs/writers/iceberg/catalog/rest?rest-catalog=polaris)

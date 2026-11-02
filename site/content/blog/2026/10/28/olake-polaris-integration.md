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
title: "Loading Operational Databases into Apache Polaris with OLake Go"
date: 2026-10-28
author: Gauri
---

## Introduction

Most guides for Apache Polaris start with a query engine: create a catalog, open Spark or Trino, and run `CREATE TABLE`.
In practice, much of the data that lands in an Iceberg lakehouse comes from operational databases such as PostgreSQL,
MySQL or MongoDB, and it keeps changing after the first load.

[OLake Go](https://olake.io/docs) is an open-source, Apache-2.0 licensed EL (Extract–Load) tool that replicates
operational databases into Apache Iceberg tables, with incremental sync, change data capture (CDC) and resumable syncs.
This post shows how to keep such tables in sync with OLake Go, and how Polaris features apply to a writer that is not
a query engine: principals and grants, the Iceberg REST commit protocol, and table maintenance policies.

## Architecture

```
Database ──(change data capture)────> OLake Go ──(Iceberg REST)──> Polaris
                                         │                            │
                                         └───── data files ──> S3 <───┘
                                                                ▲
                                         Trino / Spark ─────────┘ (via Polaris)
```

OLake Go reads a full snapshot of each selected table, then follows the database change log: logical replication on
PostgreSQL, the binlog on MySQL or the oplog on MongoDB (see the [supported sources](https://olake.io/docs/connectors/overview)).
It writes Parquet data and delete files to object storage and commits them to Polaris through the Iceberg REST API.
Query engines see the new data after each commit, through the same catalog and the same access controls.

## Connecting OLake Go to Polaris

OLake Go uses the standard Iceberg REST client, so its configuration looks like that of any other Iceberg client:

```json
{
  "type": "ICEBERG",
  "writer": {
    "catalog_type": "polaris",
    "rest_catalog_url": "http://polaris:8181/api/catalog",
    "iceberg_s3_path": "quickstart_catalog",
    "rest_auth_type": "oauth2",
    "oauth2_uri": "http://polaris:8181/api/catalog/v1/oauth/tokens",
    "credential": "<client_id>:<client_secret>",
    "scope": "PRINCIPAL_ROLE:ALL",
    "aws_region": "us-west-2",
    "rest_access_delegation": "vended-credentials"
  }
}
```

`iceberg_s3_path` carries the Polaris catalog name and is sent as the REST `warehouse` parameter, so the
storage location and storage configuration come from the catalog definition in Polaris.
With `rest_access_delegation` set to `vended-credentials`, OLake Go needs no storage keys of its own: Polaris returns
short-lived, scoped credentials with each table it loads.
All options are described in the [OLake Go Polaris catalog documentation](https://olake.io/docs/writers/iceberg/catalog/rest?rest-catalog=polaris&rest-auth-type=oauth2),
and the [Docker CLI documentation](https://olake.io/docs/install/docker-cli) shows how to run a sync with this configuration.

A runnable setup with PostgreSQL, RustFS and Trino is available in the
[Polaris getting started guide for OLake Go](https://polaris.apache.org/guides/olake/).

## A dedicated principal for ingestion

Ingestion jobs should not run as `root`. Polaris RBAC makes it straightforward to give a replication job exactly
the rights it needs on one catalog:

```shell
polaris principals create olake_ingest

polaris principal-roles create ingest
polaris principal-roles grant --principal olake_ingest ingest

polaris catalog-roles create --catalog quickstart_catalog writer
polaris catalog-roles grant --catalog quickstart_catalog --principal-role ingest writer

polaris privileges catalog grant --catalog quickstart_catalog --catalog-role writer CATALOG_MANAGE_CONTENT
```

Readers such as analysts or BI tools can be given a separate catalog role with `TABLE_READ_DATA` only.
Because the writer and the readers go through the same catalog, there is one place to audit who can change a
table and who can read it.

## Change data capture and Iceberg commits

Each change replaces the previous version of a row: OLake Go writes an equality delete file for the old version and a
data file with the new one, so no Spark job is needed to merge them. Rows deleted at the source are kept as
markers with `_op_type = 'd'`, as described in the [PostgreSQL connector documentation](https://olake.io/docs/connectors/postgres).
Polaris validates every commit against the current table metadata, which keeps concurrent writers, for example an
ingestion job and a maintenance job, from overwriting each other's snapshots.

Frequent commits do have a cost: many small data files and delete files. That is where table maintenance comes in.

## Table maintenance with Polaris policies

Polaris lets you attach maintenance policies such as `system.data-compaction` and `system.snapshot-expiry` to a
catalog, namespace or table. A policy attached to the namespace OLake Go writes into applies to every replicated
table, including tables added later:

```json
{
  "name": "cdc-compaction",
  "type": "system.data-compaction",
  "content": "{\"version\": \"2025-02-03\", \"enable\": true, \"config\": {\"target_file_size_bytes\": 134217728}}"
}
```

Polaris stores and resolves policies but does not run maintenance itself. Compaction is performed by an external
engine or service, for example Spark's Iceberg maintenance procedures or a table maintenance tool such as
[OLake Fusion](https://olake.io/docs/fusion/maintenance/catalogs), which supports Polaris catalogs.

## Conclusion

Polaris works for writers that are not query engines in the same way it does for Spark or Trino. A replication tool
authenticates as a principal, commits through the Iceberg REST API, and is governed by the same grants and policies
as every other client. That keeps ingestion, querying and maintenance on one catalog.

## Resources

- [OLake Go on GitHub](https://github.com/datazip-inc/olake)
- [OLake Go documentation: Polaris catalog](https://olake.io/docs/writers/iceberg/catalog/rest?rest-catalog=polaris&rest-auth-type=oauth2)
- [Polaris policies](https://polaris.apache.org/in-dev/unreleased/policy/)
- [Polaris access control](https://polaris.apache.org/in-dev/unreleased/managing-security/access-control/)

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
title: "An Incremental Order-Book Lakehouse with Apache Polaris, Apache Iceberg, Spark and SeaweedFS as S3 Storage"
date: 2026-11-20
author: Alejandro Pajaron
---

> **TL;DR** — [orderbook-lakehouse](https://github.com/vanalex/orderbook-lakehouse) is a small,
> fully local lakehouse for market order-book events. Events arrive over HTTP, land in
> S3-compatible storage, and flow through bronze, silver and gold Iceberg tables managed by
> Apache Polaris. Every step is incremental and safe to rerun, and the pipeline keeps its
> progress in Iceberg snapshot metadata, committed through Polaris together with the data.
> This post covers how Polaris fits into that design, and the settings needed to run it on
> non-AWS object storage.

---

## Why an Order Book?

Order-book data is a good stress test for a lakehouse. It's a high-volume stream of small
events (`add`, `cancel`, `trade`, `snapshot`), producers retry and send duplicates, and the
interesting outputs (one-minute OHLCV bars, the best bid and ask, the running state of the
book) are stateful. A pipeline that gets this right has to answer three questions:

1. How do I read only the data that's new since the last run?
2. How do I make a rerun, or a run that crashed halfway, produce the same result?
3. How do I keep the tables healthy while all those small incremental commits pile up?

Apache Iceberg's snapshots answer the first two, and Apache Polaris, as the catalog every
writer commits through, is where those snapshots become the single source of truth.

## The Stack

Everything runs with `docker compose up -d`. Every image is pinned:

```
Producer ──HTTP──► Redpanda Connect (:4195)
                     │  NDJSON files → s3://orderbook-warehouse/landing/raw_events/
                     ▼
Spark (Scala, local mode)  reads landed files, writes Iceberg tables
   │  Iceberg REST protocol: catalog + metadata
   ▼
Apache Polaris 1.8.0 (:8181) ──► PostgreSQL 17 (catalog state)
   │  S3 API
   ▼
SeaweedFS (:8333)           S3-compatible storage, bucket orderbook-warehouse
```

The responsibilities are split cleanly:

- **SeaweedFS** stores every data file and every Iceberg metadata file.
- **PostgreSQL** stores only Polaris's own state: realms, principals, catalogs, namespaces, and,
  for each table, a pointer to its current `metadata.json`.
- **Polaris** is the one place that answers "which metadata file is the current version of this
  table?", and it's the server that performs every commit.
- **Spark** asks Polaris for metadata, but reads and writes data files directly in object storage.

Two one-off containers prepare Polaris. `polaris-bootstrap` runs the Polaris admin tool to
create the realm and the root principal in PostgreSQL, and `polaris-init` calls the management
API to create the `orderbook` catalog and its grants. Both are idempotent, so the stack can be
restarted at any time, and `docker compose down -v` wipes the storage and the catalog together
so they never get out of sync.

## Running Polaris on Non-AWS S3 Storage

Getting Spark to commit Iceberg tables through Polaris to a local S3-compatible store took a few
settings that aren't obvious the first time. I hope this list saves someone an afternoon.

### 1. No STS: skip credential subscoping

SeaweedFS has no STS, so Polaris can't vend scoped credentials. The catalog is created with
`stsUnavailable: true`:

```json
{
  "catalog": {
    "name": "orderbook",
    "type": "INTERNAL",
    "properties": { "default-base-location": "s3://orderbook-warehouse" },
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": ["s3://orderbook-warehouse/"],
      "endpoint": "http://seaweedfs:8333",
      "pathStyleAccess": true,
      "stsUnavailable": true
    }
  }
}
```

and the server runs with the `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION` feature enabled. Spark then
authenticates to object storage with its own static keys.

### 2. Point Polaris's own S3 client at the local endpoint

With a REST catalog, the server performs the commit: Polaris itself writes each new
`metadata.json` before it moves the table pointer. So Polaris needs to reach the object store
too, not just the clients. Without an explicit endpoint its S3 client goes to real AWS and table
commits fail with `403 The AWS Access Key Id you provided does not exist`. The fix is in the
`polaris` service's environment:

```yaml
AWS_ACCESS_KEY_ID: admin
AWS_SECRET_ACCESS_KEY: password
AWS_REGION: us-east-1
AWS_ENDPOINT_URL_S3: http://seaweedfs:8333
AWS_ENDPOINT_URL_STS: http://seaweedfs:8333
polaris.features."SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION": "true"
polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES": '["S3"]'
```

### 3. Accept virtual-hosted-style bucket addresses

In this setup, Polaris's server-side requests addressed the bucket virtual-hosted style
(`orderbook-warehouse.seaweedfs`). Two small changes make that work in Docker: SeaweedFS
starts with `-s3.domainName=seaweedfs`, and the container gets the network alias
`orderbook-warehouse.seaweedfs` so the hostname resolves.

### 4. Register S3A next to S3FileIO

Iceberg's table reads and writes go through `S3FileIO`, but the Spark action behind
`remove_orphan_files` lists files through Hadoop's `FileSystem` API. Spark's streaming checkpoints
do too. So the Spark session also registers `S3AFileSystem`, against the same endpoint and keys.

### 5. Bootstrap Polaris explicitly when using PostgreSQL

With the relational JDBC persistence, the schema is created up front (an init script in the
PostgreSQL container), and the realm and root principal come from the admin tool's `bootstrap`
command. Keep `apache/polaris` and `apache/polaris-admin-tool` on the same version.

## Connecting Spark to Polaris

The Spark side is a standard Iceberg REST catalog configuration with OAuth2 client credentials:

```scala
val cat = "spark.sql.catalog.orderbook"

SparkSession.builder()
  .config("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .config(cat, "org.apache.iceberg.spark.SparkCatalog")
  .config(s"$cat.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
  .config(s"$cat.uri", "http://localhost:8181/api/catalog")
  .config(s"$cat.warehouse", "orderbook")
  .config(s"$cat.rest.auth.type", "oauth2")
  .config(s"$cat.oauth2-server-uri", "http://localhost:8181/api/catalog/v1/oauth/tokens")
  .config(s"$cat.credential", "root:s3cr3t")
  .config(s"$cat.scope", "PRINCIPAL_ROLE:ALL")
  .config(s"$cat.header.Polaris-Realm", "default-realm")
  .config(s"$cat.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
  .config(s"$cat.s3.endpoint", "http://localhost:8333")
  .config(s"$cat.s3.path-style-access", "true")
  // ... S3 keys, region, and the S3A settings from step 4
  .getOrCreate()
```

These addresses assume Spark runs on the host. A Spark process inside the Compose network would
use the service names (`polaris:8181`, `seaweedfs:8333`) instead.

`make spark-smoke-test` creates a table, writes two rows, reads them back and drops it, which
checks the whole chain from Spark to Polaris to SeaweedFS in a few seconds.

## A Commit Is a Pointer Swap

The clearest way to see what the catalog does is to watch it commit. `make pointers` reads
Polaris's table rows in PostgreSQL directly. Each table's row holds a `metadata-location` and an
`entity_version`. Run it before and after a write to the silver table, and you'll see:

- `entity_version` goes from 1 to 2,
- `metadata-location` moves from `00000-….metadata.json` to `00001-….metadata.json`.

That's the whole commit: Polaris writes the new metadata file to object storage, then atomically
swaps the pointer, and only one writer can win the swap. Everything else about the table (schema,
partitioning, snapshots, the summary properties discussed below) lives in that metadata file.

Reading Polaris's database is a good way to see what's going on inside, but it isn't an interface
to build on: applications should go through the Iceberg REST API, as `make snapshots` does below.

## Bronze, Silver, Gold

| Layer | Table | What it does |
|---|---|---|
| Bronze | `bronze.raw_events` | Appends the NDJSON files that landed since the last run, using Spark Structured Streaming with `Trigger.AvailableNow`. Stores the data as received: every column nullable, with `_source_file` and `_ingested_at`. |
| Silver | `silver.book_events` | Drops malformed rows, dedupes on `(instrument, seq_no)`, and `MERGE`s into silver with an insert-only `WHEN NOT MATCHED` clause. Partitioned by instrument and date. Stops if more than 50% of a batch is malformed. |
| Gold | `gold.ohlcv_bars_1m`, `gold.top_of_book_snapshots`, `gold.book_state` | Builds one-minute OHLCV bars and the best bid and ask each minute, by keeping a running quantity for every price level. The running book is saved in `gold.book_state`. |

On top of gold, nine Spark SQL jobs answer trading questions: spreads, VWAP, realized and
Parkinson volatility, whether book imbalance predicts the next bar, and a cancel-to-add ratio as
a basic surveillance signal. Since the tables are in an Iceberg REST catalog, any engine that
speaks the protocol (Trino, DuckDB, PyIceberg) could run the same queries.

## Incremental Reads with Watermarks in Snapshot Summaries

Each downstream job needs to remember which upstream snapshot it processed last. Keeping that in
a side table or a file would create a second commit that can disagree with the first after a
crash. Instead, the watermark goes into the **summary of the job's own output commit**, using
Iceberg's `CommitMetadata`:

```scala
def withCommit[T](watermarks: (String, Long)*)(write: => T): T = {
  val properties = watermarks.map { case (name, id) =>
    s"pipeline.watermark.$name" -> id.toString
  }.toMap.asJava
  CommitMetadata.withCommitProperties[T, RuntimeException](
    properties, () => write, classOf[RuntimeException])
}
```

Silver's `MERGE` commit therefore carries `pipeline.watermark.bronze_snapshot_id`, and Polaris
commits the data and the watermark in a single pointer swap: both land or neither does. The next
run reads the watermark back from the sink table's snapshot history and reads only the changes
since then:

```scala
spark.read
  .option("start-snapshot-id", watermark)
  .option("end-snapshot-id", currentBronzeSnapshot)
  .table("orderbook.bronze.raw_events")
```

Because the watermark is just part of the table metadata, you can inspect it without Spark.
`make snapshots` asks Polaris for the table over the REST API and prints the history:

```text
silver.book_events
  watermark  bronze_snapshot_id = 6195386220912737740
  committed (UTC)       snapshot_id           operation  added  total
  …                     698872475013876825    append     5000   5000
  …                     8127957727737915548   append     5000   10000
  …                     647505745747064067    append     0      10000
```

One detail worth knowing: with only a `WHEN NOT MATCHED THEN INSERT` clause, Iceberg runs the
`MERGE` as an anti-join plus an append, so it shows up as an `append` snapshot and rewrites no
existing files. That matters for gold, because Iceberg's incremental reads only follow append
snapshots. It also means a `MERGE` that inserts nothing still commits, which is exactly what
moves the watermark forward.

## Duplicates Are Handled in Layers

Delivery is at least once, so duplicates are expected. Each layer catches a different kind:

| Where | Catches | How |
|---|---|---|
| Bronze ingest | The same landed file read twice | The streaming checkpoint records which files were loaded |
| Silver `clean` | The same event twice in one batch | `dropDuplicates("instrument", "seq_no")` |
| Silver `MERGE` | An event that's already in silver | `WHEN NOT MATCHED THEN INSERT` on `(instrument, seq_no)` |

`make demo-dedup` replays the last 5000 events, as a producer retry would. Bronze appends them,
because it stores what arrived. Silver reads them and reports:

```text
Read 5000 new bronze events (through snapshot …), 5000 clean;
inserted 0 into orderbook.silver.book_events, skipped 5000 already there
```

Gold works the same way. Each run recomputes only the minutes its new silver rows touch and
`MERGE`s them in by `(instrument, window_start)`. `gold.book_state`, the one output that can't be
recomputed safely, is written last, in the commit that carries gold's watermark. If a run dies
before that commit, the next run starts from the same point and rewrites the same minutes with
the same values. This is recovery by rerunning, not a transaction across the gold tables: each
table commits separately, so a query during a run, or after a crash, can see the bars updated
before the book is.

## Maintenance Goes Through the Catalog Too

Every incremental run adds a snapshot and some small files, so `make maintain` runs Iceberg's
maintenance actions on every table: data-file compaction, manifest rewrites, snapshot expiry and
orphan-file cleanup. Each one commits through Polaris like any other writer.

There's one subtlety. Expiring snapshots can remove the very snapshot a downstream watermark
points at, which would force the next run into a full re-read. To avoid that, the maintenance
rewrites carry the watermarks forward into their own commits, and record which data snapshot
they rewrote (`pipeline.data-snapshot-id`). A rewrite adds no rows, so a downstream job can
safely resume from it:

```text
Watermark: orderbook.silver.book_events's 'bronze_snapshot_id' (…) was expired by maintenance —
resuming from rewrite snapshot …
```

On the demo data, compacting bronze after the dedup demo takes it from 3 snapshots and 2 data
files to 2 snapshots and 1 file, and silver and gold keep reading incrementally afterwards.

A rewrite commit doesn't bring back expired history, though. If new data was appended upstream
and the snapshots holding it were expired before a downstream job read them, that job can't
resume incrementally and falls back to a full read. The jobs handle that correctly, but it's
slower. In production, snapshot retention has to cover how far behind the slowest downstream
job can fall: maintenance has to know what its readers still need.

## Testing It Without Docker

`PipelineIntegrationSpec` runs the real jobs end to end against a local Iceberg catalog in a
temporary directory. It checks that incremental runs equal a full recompute, that reruns commit
nothing new, that duplicates and malformed events are absorbed, that a gold run that crashes
midway is recovered, and that maintenance doesn't force a full re-read. The transform functions
themselves do no I/O, so they're unit-tested on small in-memory DataFrames.

These tests use a local Iceberg catalog, not Polaris, so they check the pipeline logic rather than
the catalog integration. `make spark-smoke-test` and the demo steps cover the live
Spark–Polaris–SeaweedFS path. Concurrent writers and failure injection against the full Docker
stack aren't tested yet.

## Try It Yourself

You need Docker, a JDK and sbt:

```sh
git clone https://github.com/vanalex/orderbook-lakehouse.git
cd orderbook-lakehouse
docker compose up -d
make health && make catalogs     # Polaris is up and the catalog exists
make spark-smoke-test            # Spark → Polaris → SeaweedFS
make setup                       # namespaces and empty tables
make generate                    # POST 5000 synthetic events
make pipeline                    # bronze → silver → gold
make query-spread                # or any other make query-*
make snapshots TABLE=silver.book_events
```

The repository's `GUIDE.md` walks through each step, including the dedup and maintenance demos.

## Limitations

This is a demo, not a production setup. Credentials are hardcoded, there's no STS, and jobs are
run by hand rather than by a scheduler. `modify` events aren't applied to the book, because the
event schema has no `order_id`. The gold tables aren't committed atomically together, and the
pipeline assumes a single writer per table.

## Wrapping Up

The main takeaway for me was how much a REST catalog simplifies pipeline state. Because Polaris
performs every commit as a single pointer swap, the pipeline can keep its progress in the same
commit as its data, and every step (ingest, merge, aggregation, maintenance) becomes safe to
rerun without a separate state store.

Feedback and contributions are welcome on
[GitHub](https://github.com/vanalex/orderbook-lakehouse). To learn more about Polaris, see the
[Apache Polaris documentation](https://polaris.apache.org/) or join the community on the
[dev mailing list](https://polaris.apache.org/community/).

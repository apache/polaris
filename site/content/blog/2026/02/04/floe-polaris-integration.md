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
title: "Floe and Apache Polaris: Policy-Driven Table Maintenance for Apache Iceberg"
date: 2026-02-04
author: Neelesh Salian
---

## Introduction

Iceberg tables accumulate technical debt over time. Small files multiply as streaming jobs append data in micro-batches. Delete files pile up from CDC workloads. Snapshots grow unbounded, bloating metadata. Without regular maintenance, query performance degrades, storage costs rise, and planning times stretch from milliseconds to seconds.

Apache Polaris provides a vendor-neutral Iceberg catalog with governance and access control, but it does not execute maintenance operations. The catalog manages metadata and enforces permissions. Compaction, snapshot expiration, orphan cleanup, and manifest optimization remain the user's responsibility.

[Floe](https://github.com/nssalian/floe) fills that gap. It connects to Polaris, discovers tables, evaluates their health, and orchestrates maintenance through policy-driven automation. Instead of writing custom scripts or manually running Spark jobs, you define policies that specify what maintenance to perform, which tables to target, and under what conditions to trigger execution. Floe handles the rest: scheduling, execution via Spark or Trino, and tracking outcomes.

## Architecture

Polaris remains the source of truth for metadata and access control. Floe reads the catalog, evaluates policies, triggers maintenance on your chosen engine, and records outcomes.

![Polaris + Floe Architecture](/img/blog/2026/02/04/high_level_architecture.png)

### Data Flow

1. **Policy discovery**: Floe loads enabled policies and matches them to tables.
2. **Health assessment**: Floe evaluates table health based on scan mode and thresholds.
3. **Planning & gating**: The planner selects operations; trigger conditions decide if they run.
4. **Execution**: The orchestrator dispatches operations to Spark or Trino.
5. **Persistence**: Results and health history are stored for tracking and recommendations.

## Quick Start

```bash
git clone https://github.com/nssalian/floe
cd floe
make example-polaris
```

This starts Polaris, MinIO, and Floe with a `demo` catalog, creates sample Iceberg tables, and configures demo policies.

* Floe UI: http://localhost:9091/ui
* Floe API: http://localhost:9091/api/

For Trino instead of Spark, run `make clean` first, then `make example-polaris-trino`.

## Configuration

```bash
FLOE_CATALOG_TYPE=POLARIS
FLOE_CATALOG_NAME=demo
FLOE_CATALOG_POLARIS_URI=http://polaris:8181/api/catalog
FLOE_CATALOG_POLARIS_CLIENT_ID=root
FLOE_CATALOG_POLARIS_CLIENT_SECRET=secret
FLOE_CATALOG_WAREHOUSE=demo
```

Note: For Polaris, `FLOE_CATALOG_WAREHOUSE` is the catalog name, not an S3 path.

## Defining Policies

Policies define maintenance operations and target tables via patterns:

```bash
curl -s -X POST "http://localhost:9091/api/v1/policies" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders-maintenance",
    "tablePattern": "demo.test.*",
    "priority": 50,
    "rewriteDataFiles": {
      "strategy": "BINPACK",
      "targetFileSizeBytes": 134217728
    },
    "expireSnapshots": {
      "retainLast": 10,
      "maxSnapshotAge": "P7D"
    },
    "orphanCleanup": {
      "retentionPeriodInDays": 3
    },
    "rewriteManifests": {}
  }'
```

Operations: `rewriteDataFiles`, `expireSnapshots`, `orphanCleanup`, `rewriteManifests`.

## Triggering Maintenance

```bash
curl -X POST http://localhost:9091/api/v1/maintenance/trigger \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": "demo",
    "namespace": "test",
    "table": "orders"
}'
```

Monitor progress via UI at `/ui/operations` or API at `/api/v1/operations`.

## Floe UI

Floe includes a web UI for managing policies and monitoring table health. The table view shows metadata alongside health indicators (snapshot count, small file percentage, delete file ratio) so you can see at a glance which tables need attention:

![Table Metadata View](/img/blog/2026/02/04/table_metadata.png)

## Health Reporting

```bash
curl http://localhost:9091/api/v1/tables/test/orders/health
```

Reports include: snapshot count/age, small file percentage, delete file count, partition skew, manifest size.

Scan modes: `metadata` (default), `scan`, `sample`.

```properties
floe.health.scan-mode=metadata
floe.health.sample-limit=10000
floe.health.persistence-enabled=true
floe.health.max-reports-per-table=100
```

The `metadata` mode is fast but only sees file-level statistics. Use `scan` or `sample` when you need accurate small-file detection based on actual file sizes.

## Automated Scheduling

The scheduler computes a debt score per table based on health issues, time since last maintenance, and failure rate. Higher scores are prioritized.

```properties
floe.scheduler.enabled=true
floe.scheduler.max-tables-per-poll=10
floe.scheduler.max-bytes-per-hour=10737418240
floe.scheduler.failure-backoff-threshold=3
floe.scheduler.failure-backoff-hours=6
floe.scheduler.zero-change-threshold=5
floe.scheduler.condition-based-triggering-enabled=true
```

Key tuning parameters:
- `max-bytes-per-hour`: Caps total bytes rewritten to avoid overwhelming storage I/O
- `failure-backoff-threshold` / `failure-backoff-hours`: Prevents repeatedly retrying failing tables
- `zero-change-threshold`: Reduces frequency for tables that consistently have no work to do

## Signal-Based Triggering

Gate execution based on table health instead of pure cron:

```bash
curl -s -X POST "http://localhost:9091/api/v1/policies" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "smart-compaction",
    "tablePattern": "demo.test.*",
    "priority": 100,
    "rewriteDataFiles": {
      "strategy": "BINPACK",
      "targetFileSizeBytes": 134217728
    },
    "triggerConditions": {
      "smallFilePercentageAbove": 20,
      "deleteFileCountAbove": 50,
      "minIntervalMinutes": 60
    }
  }'
```

Triggers when any condition is met (default OR logic) or when all conditions are met if `triggerLogic` is set to `AND`, and the min interval has elapsed.

For critical tables, force execution when max delay is exceeded:

```json
{
  "triggerConditions": {
    "smallFilePercentageAbove": 30,
    "criticalPipeline": true,
    "criticalPipelineMaxDelayMinutes": 360
  }
}
```

Policies without `triggerConditions` run whenever the scheduler picks them up, preserving the original behavior.

## Execution Engines

Floe supports Spark (via Livy) and Trino as execution engines.

Spark configuration:
```bash
FLOE_ENGINE_TYPE=SPARK
FLOE_LIVY_URL=http://livy:8998
```

Trino configuration:
```bash
FLOE_ENGINE_TYPE=TRINO
FLOE_TRINO_JDBC_URL=jdbc:trino://trino:8080
FLOE_TRINO_CATALOG=demo
```

## Security

* Enable authentication: `FLOE_AUTH_ENABLED=true`
* Floe uses its own storage credentials; Polaris credentials are only used for catalog access
* Run Floe in the same network as Polaris and engines

## Conclusion

Apache Polaris and Floe complement each other well. Polaris provides the catalog layer (metadata management, access control, credential vending) while Floe provides the maintenance layer that keeps Iceberg tables healthy and performant. Together they give you centralized governance, automated maintenance, health visibility, and flexible execution on Spark or Trino.

Run `make example-polaris` to try the integration locally, or check out the [Floe documentation](https://nssalian.github.io/floe/) for deployment options.

## Resources

* [Apache Polaris](https://polaris.apache.org/)
* [Floe Project](https://github.com/nssalian/floe)
* [Iceberg Table Maintenance](https://iceberg.apache.org/docs/latest/maintenance/)

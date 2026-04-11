<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->
# Design: Multi-DataSource support in JDBC persistence

## Goal
The goal of this design is to decouple `DataSource` selection from the core JDBC persistence logic. This allows for:
1.  **Workload Isolation**: Separating the Metastore, Metrics reporting, and Event logging into different physical databases or connection pools.
2.  **Scalable Multi-Tenancy**: Enabling per-realm (tenant) routing to support large-scale deployments.

## Core Interface: `DataSourceResolver`
A service interface designed to resolve the correct `DataSource` based on the workload's metadata.

```java
public interface DataSourceResolver {
  enum StoreType {
    METASTORE,
    METRICS,
    EVENTS
  }

  DataSource resolve(RealmContext realmContext, StoreType storeType);
}
```

### Key Components
- **`DataSourceResolver`**: SPI for resolution logic.
- **`DefaultDataSourceResolver`**: Backward-compatible implementation that returns the single primary `DataSource` for all requests.
- **`JdbcMetaStoreManagerFactory`**: Overwrites the resolution logic to use the `DataSourceResolver`.
- **`JdbcBasePersistenceImpl`**: Manages three separate `DatasourceOperations` objects.

## Schema Management and Evolution

### Functional SQL Script Splitting
To support hosting different workloads on different databases, the monolithic `schema-vX.sql` scripts are split into functional components:
- `schema-vX-metastore.sql`: Definitions for `polaris_entities`, `polaris_grant_records`, and `polaris_schema_version`.
- `schema-vX-metrics.sql`: Definitions for `polaris_metrics_scan` and `polaris_metrics_commit`.
- `schema-vX-events.sql`: Definitions for `polaris_events`.

### Version Authority
The **Metastore** remains the single source of truth for the realm's logical schema version. The `polaris_schema_version` table is exclusively maintained within the `METASTORE` data source. All associated stores (Metrics, Events) are expected to be physically compatible with this detected version.

## Operational Behaviors

### Bootstrap
When a realm is bootstrapped via `JdbcMetaStoreManagerFactory.bootstrapRealms()`:
1.  **Resolution**: The `DataSourceResolver` is called for each `StoreType` (METASTORE, METRICS, EVENTS).
2.  **Initialization**: Each resolved data source is initialized with its specific functional SQL script.
3.  **Idempotency**: If all three `StoreType` mappings point to the same physical database, the scripts are applied sequentially. The DDL is designed to be idempotent to prevent errors during this "merged" initialization.

### Purge (Cleanup)
When a realm is purged:
1.  The `purge()` operation is initiated.
2.  The `JdbcBasePersistenceImpl.deleteAll()` method is triggered.
3.  **Multi-Store Cleanup**: `deleteAll()` executes `DELETE` commands targeting the specific `realm_id` across all three `DatasourceOperations`:
    - `metastoreOps`: Clears entities, grants, secrets, and policy mappings.
    - `metricsOps`: Clears `scan_metrics_report` and `commit_metrics_report`.
    - `eventOps`: Clears the `events` table.
4.  This ensures that all data across all three potential data sources is cleaned up for the specified `realm_id`.

## Schema Upgrades
Upgrading a Polaris deployment from version N to N+1 involves:
1.  **Detection**: The service detects that the `metastore`'s `polaris_schema_version` is below the requested level.
2.  **Execution**: The migration logic resolves each `StoreType` and applies the relevant "vN-to-vN+1" upgrade script.
3.  **Consistency**: Because the process is unified within the `JdbcMetaStoreManagerFactory`, it provides a single point of failure and ensures that all data sources are either upgraded or rolled back (where possible).

## Scalable Multi-Tenancy
The SPI allows for sophisticated implementations that can:
- Route "Enterprise" realms to dedicated high-performance clusters.
- Move realms between physical databases by updating the resolver's internal mapping metadata (e.g., during a maintenance window).

## Benefits
- **Zero Impact on Existing Users**: The default configuration maintains a single-datasource model.
- **Future-Proof**: Provides the necessary hooks for table-level connection pool separation and physical database sharding.
- **Maintainable**: The functional script split clearly defines the table categories and their associated workloads.

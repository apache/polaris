# Using Apache Polaris as a Catalog for Apache Hudi Tables



## What is Apache Hudi?

[Apache Hudi](https://hudi.apache.org/) (Hadoop Upserts Deletes and Incrementals) is an open-source
data lakehouse platform that brings database-like capabilities to the data lake. Hudi was originally
created at Uber to solve large-scale streaming ingestion and is widely adopted across the industry.

Key differentiators:

- **Incremental processing** — first-class support for incremental reads and writes, enabling
  efficient pipelines that only process changed data.
- **Table types** — Merge-on-Read (MOR) tables optimise for write-heavy workloads; Copy-on-Write
  (COW) tables optimise for read-heavy workloads.
- **Built-in table services** — clustering, compaction, and cleaning run as managed services,
  reducing operational overhead.
- **Record-level change data capture (CDC)** — track inserts, updates, and deletes at the record
  level for downstream consumers.
- **Near-real-time upserts** — streaming ingestion with sub-minute latency for mutable datasets.

## What is Apache Polaris (and why catalogs matter)?

[Apache Polaris](https://polaris.apache.org/) is an open catalog service that implements the
Apache Iceberg REST Catalog protocol as well provides a **Generic Tables API** for other popular table
formats such as Apache Hudi, Delta Lake, etc. A catalog provides centralised metadata management, engine-agnostic table discovery, and
a single place to enforce role-based access control (RBAC). Instead of every engine maintaining its
own pointer to every table, engines ask the catalog: "where is table X, and am I allowed to read
the underlying metadata and data files that belong to the table?"

Under the hood, the Polaris's Spark plugin (`SparkCatalog`) detects when a table uses the Hudi
provider and delegates Hudi-specific operations — creating the `.hoodie` by delegating to hudi's spark catalog implemenation `HoodieCatalog`, while persisting the table's catalog entry
through the Polaris REST API. 

Below we will run thru a simple example of how users can leverage these technologies for building a governed lakehouse.

## Prerequisites

| Requirement | Version | Notes                               |
|---|---|-------------------------------------|
| Java | 17+ | Required by Polaris server          |
| Apache Spark | 3.5.x  |                                     |
| Apache Hudi bundle jar | `hudi-spark3.5-bundle_2.12:1.1.1` | Single uber jar with all Hudi deps  |
| Polaris Spark client jar | `polaris-spark-3.5_2.12` | The Polaris Spark catalog plugin    |
| Docker + Docker Compose | Latest stable | For running the Polaris server      |

## Hands-on Tutorial

### Step 1 — Start Polaris

Save the following as `docker-compose.yml` and run `docker compose up -d`:

```yaml
services:

  polaris:
    image: apache/polaris:latest
    ports:
      - "8181:8181"
      - "8182:8182"
    environment:
      POLARIS_BOOTSTRAP_CREDENTIALS: POLARIS,root,s3cr3t
      polaris.realm-context.realms: POLARIS
      quarkus.otel.sdk.disabled: "true"
      polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES": '["FILE","S3","GCS","AZURE"]'
      polaris.features."ALLOW_INSECURE_STORAGE_TYPES": "true"
      polaris.readiness.ignore-severe-issues: "true"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8182/q/health"]
      interval: 2s
      timeout: 10s
      retries: 10
      start_period: 10s
```

> **Tip:** The example above uses local storage for simplicity. For S3 or other object storage,
> see the full [quickstart docker-compose.yml](getting-started/quickstart/docker-compose.yml).

Once the Polaris server is running, obtain a root token and create a catalog:

```bash
# Obtain a root access token
export TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -d 'grant_type=client_credentials' \
  -d 'client_id=root' \
  -d 'client_secret=s3cr3t' \
  -d 'scope=PRINCIPAL_ROLE:ALL' \
  | jq -r '.access_token')

# Create an INTERNAL catalog named "hudi_catalog"
curl -s -X POST http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Polaris-Realm: POLARIS" \
  -d '{
    "catalog": {
      "name": "hudi_catalog",
      "type": "INTERNAL",
      "readOnly": false,
      "properties": {
        "default-base-location": "file:///tmp/hudi_warehouse"
      },
      "storageConfigInfo": {
        "storageType": "FILE",
        "allowedLocations": ["file:///tmp/hudi_warehouse"]
      }
    }
  }'

# Grant TABLE_WRITE_DATA so Spark can create and write to tables
curl -s -X PUT http://localhost:8181/api/management/v1/catalogs/hudi_catalog/catalog-roles/catalog_admin/grants \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Polaris-Realm: POLARIS" \
  -d '{"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}'
```

### Step 2 — Configure Spark for Hudi + Polaris

Launch `spark-sql` with the Hudi bundle jar, the Polaris Spark plugin, and the required
configuration:

```bash
spark-sql \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.1.1,org.apache.polaris:polaris-spark-3.5_2.12:1.4.0-incubating \
  --conf spark.sql.catalog.polaris=org.apache.polaris.spark.SparkCatalog \
  --conf spark.sql.catalog.polaris.type=rest \
  --conf spark.sql.catalog.polaris.uri=http://localhost:8181/api/catalog \
  --conf spark.sql.catalog.polaris.warehouse=hudi_catalog \
  --conf spark.sql.catalog.polaris.token=$TOKEN \
  --conf spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar \
  --conf spark.sql.defaultCatalog=polaris
```

Configuration breakdown:

| Config | Purpose |
|---|---|
| `spark.sql.catalog.polaris` | Registers the Polaris `SparkCatalog` plugin as the `polaris` catalog |
| `spark.sql.catalog.polaris.type=rest` | Uses the REST catalog protocol |
| `spark.sql.catalog.polaris.uri` | Points to the Polaris server's catalog API |
| `spark.sql.catalog.polaris.warehouse` | Tells Polaris which catalog to use |
| `spark.sql.extensions` | Loads the Hudi session extension for DDL/DML support |
| `spark.sql.catalog.spark_catalog` | Sets `HoodieCatalog` as the default session catalog |
| `spark.serializer` / `spark.kryo.registrator` | Required by Hudi for efficient serialisation |

### Step 3 — Create a namespace and a Hudi table

```sql
CREATE NAMESPACE rides;
USE NAMESPACE rides;

CREATE TABLE rides_hudi (
  ride_id    INT,
  rider_name STRING,
  fare       DOUBLE,
  city       STRING,
  ride_date  STRING
)
USING HUDI
PARTITIONED BY (ride_date)
LOCATION 'file:///tmp/hudi_warehouse/rides/rides_hudi';
```

> **Important:** Hudi tables in Polaris require an explicit `LOCATION` clause. Creating a table
> without `LOCATION` is not currently supported and will raise an error.

### Step 4 — Insert sample data

```sql
INSERT INTO rides_hudi VALUES
  (1,  'Alice',   12.50, 'New York',      '2025-01-15'),
  (2,  'Bob',     8.75,  'San Francisco', '2025-01-15'),
  (3,  'Charlie', 15.00, 'New York',      '2025-01-16'),
  (4,  'Diana',   22.30, 'Chicago',       '2025-01-16'),
  (5,  'Eve',     9.10,  'San Francisco', '2025-01-15'),
  (6,  'Frank',   18.60, 'Chicago',       '2025-01-17'),
  (7,  'Grace',   11.20, 'New York',      '2025-01-17'),
  (8,  'Hank',    7.50,  'San Francisco', '2025-01-16'),
  (9,  'Ivy',     30.00, 'Chicago',       '2025-01-17'),
  (10, 'Jack',    14.80, 'New York',      '2025-01-15');
```

### Step 5 — Query the table

```sql
-- All rows
SELECT * FROM rides_hudi ORDER BY ride_id;
```

Expected output:

```
ride_id | rider_name | fare  | city          | ride_date
--------|------------|-------|---------------|----------
1       | Alice      | 12.5  | New York      | 2025-01-15
2       | Bob        | 8.75  | San Francisco | 2025-01-15
3       | Charlie    | 15.0  | New York      | 2025-01-16
4       | Diana      | 22.3  | Chicago       | 2025-01-16
5       | Eve        | 9.1   | San Francisco | 2025-01-15
6       | Frank      | 18.6  | Chicago       | 2025-01-17
7       | Grace      | 11.2  | New York      | 2025-01-17
8       | Hank       | 7.5   | San Francisco | 2025-01-16
9       | Ivy        | 30.0  | Chicago       | 2025-01-17
10      | Jack       | 14.8  | New York      | 2025-01-15
```

```sql
-- Filter by city and sort by fare
SELECT ride_id, rider_name, fare
FROM rides_hudi
WHERE city = 'New York'
ORDER BY fare DESC;
```

```
ride_id | rider_name | fare
--------|------------|------
3       | Charlie    | 15.0
10      | Jack       | 14.8
1       | Alice      | 12.5
7       | Grace      | 11.2
```

### Step 6 — Verify table registration in Polaris

**Via Spark SQL:**

```sql
SHOW TABLES IN rides;
```

```
namespace | tableName  | isTemporary
----------|------------|------------
rides     | rides_hudi | false
```

### Step 7 — Cleanup

```sql
DROP TABLE rides_hudi;
DROP NAMESPACE rides;
```

Then stop the Polaris server:

```bash
docker compose down
```

## Current Limitations

The Polaris Hudi integration is under active development. As of today, the following limitations
apply:

| Operation | Status | Details |
|---|---|---|
| `CREATE TABLE ... USING HUDI` (with `LOCATION`) | Supported | Must include an explicit `LOCATION` clause |
| `CREATE TABLE ... USING HUDI` (without `LOCATION`) | Not supported | Raises `UnsupportedOperationException` |
| `CREATE TABLE ... AS SELECT` (CTAS) | Not supported | Raises `IllegalArgumentException` |
| `ALTER TABLE ... RENAME TO` | Not supported | Raises `UnsupportedOperationException` |
| `ALTER TABLE ... SET LOCATION` | Not supported | Raises `UnsupportedOperationException` |
| Credential vending | Not supported | Generic tables do not yet support credential vending; configure storage credentials directly in Spark |

## Next Steps

The Polaris community is actively working on closing these gaps:

- **Credential vending for generic tables** — enabling Polaris to vend short-lived storage
  credentials for Hudi, Delta, and other generic table formats, bringing them to parity with the
  Iceberg integration for governed data access. .

If you want to get involved, check out the [Apache Polaris repository](https://github.com/apache/polaris)
and join the community discussion.

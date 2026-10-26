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
title: Default table location object storage prefix
linkTitle: Object storage table prefix
type: docs
weight: 160
---

Polaris can place **default** Iceberg table and view locations under a **per-table hash prefix**
directly beneath the catalog warehouse, instead of nesting each table only under its parent
namespace path. That behavior is controlled by the catalog feature
`DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED`.

This page explains what the flag does, how it differs from Iceberg's own object-store layout, which
other catalog settings must be enabled, and when operators might combine the two approaches.

For the raw feature flag reference, see
[`DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED`]({{% ref "configuration-reference.md" %}}#features--behavior)
in the configuration reference (`polaris.config.default-table-location-object-storage-prefix.enabled`).

## What problem it targets

Object stores such as Amazon S3 scale request rates using **prefixes** in the key space. When many
tables in the same namespace share a long common path prefix (for example
`warehouse/mydb/`), concurrent creates and metadata operations can concentrate load on a narrow
part of the key space.

With `DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED` enabled, when Polaris generates the location of
a new table or view, it places it under a **stable hash directory derived from the table identifier**
directly beneath the catalog's default base location, then appends the namespace levels and the table
name. **Different tables** therefore start under **different hash prefixes**, which can spread load
across the warehouse when many tables are active in the same namespace.

{{< alert note >}}
Amazon documents per-prefix request scaling for S3, but the exact definition of a "partitioned prefix"
is not fully specified. Polaris does not ship benchmarks proving throughput gains for this layout;
treat this flag as a **layout option** to evaluate in your environment, not a guaranteed performance
fix.
{{< /alert >}}

## What it does *not* do

- **Per-file entropy.** All data and metadata files for one table still share the **same** table-level
  hash prefix. Heavy write traffic to a **single** table is not spread across many prefixes by this
  feature alone.
- **Client-specified locations.** If the create request supplies an explicit `location`, Polaris does
  not rewrite it; the prefix applies only when Polaris computes the default location.
- **Namespace locations.** Namespace locations are unchanged; only the generated locations of tables
  and views are affected.
- **Existing tables.** Enabling the flag does not move existing tables or views. Only tables and views
  created afterwards without an explicit location get the prefix.

## Comparison with Iceberg `write.object-storage.enabled`

These features operate at different layers and are **complementary**, not equivalent.

| | Polaris `DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED` | Iceberg `write.object-storage.enabled` ([Object Store File Layout](https://iceberg.apache.org/docs/latest/aws/#object-store-file-layout)) |
|---|---|---|
| **Scope** | Polaris default **table location** under the warehouse | **Per data file** path under the table's data directory |
| **Entropy** | One hash prefix **per table** (from the table identifier) | A new hash segment **per file** |
| **Typical goal** | Separate **tables** in key space when defaults are used | Reduce hotspots for **files within a table** |
| **Configured on** | Polaris catalog feature / `polaris.config.default-table-location-object-storage-prefix.enabled` | Table properties (often via catalog defaults) |

### Example paths

For a table `newdb.newtable` and warehouse `s3://bucket/warehouse`, with two data files:

**Standard layout (neither feature):**

```text
s3://bucket/warehouse/newdb/newtable/data/file1.parquet
s3://bucket/warehouse/newdb/newtable/data/file2.parquet
```

**Iceberg object store layout only** (per-file hashes under `data/`):

```text
s3://bucket/warehouse/newdb/newtable/data/0011/0100/1011/11101010/file1.parquet
s3://bucket/warehouse/newdb/newtable/data/0011/0001/0001/00000001/file2.parquet
```

**Polaris object storage prefix only** (one hash per table under the warehouse):

```text
s3://bucket/warehouse/1111/1111/0100/01010000/newdb/newtable/data/file1.parquet
s3://bucket/warehouse/1111/1111/0100/01010000/newdb/newtable/data/file2.parquet
```

(The hash values shown are illustrative. Polaris computes the hash from the table identifier, so it is
the same every time for a given identifier and differs between tables.)

**Both enabled** (table-level prefix plus per-file layout under `data/`):

```text
s3://bucket/warehouse/1111/1111/0100/01010000/newdb/newtable/data/0011/0100/1011/11101010/file1.parquet
s3://bucket/warehouse/1111/1111/0100/01010000/newdb/newtable/data/0011/0001/0001/00000001/file2.parquet
```

## Related Polaris catalog settings

`DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED` is **off by default**. Because prefixed tables no
longer sit under their parent namespace's location, Polaris requires other location settings to be
compatible. If they are not, creating a table or view without an explicit location fails.

| Feature | Why it matters |
|---------|----------------|
| `ALLOW_UNSTRUCTURED_TABLE_LOCATION` | **Required.** Allows table locations outside the parent namespace's location. Note that it also allows callers to specify such locations themselves. |
| `ALLOW_TABLE_LOCATION_OVERLAP` | Off by default, which keeps Polaris's location-overlap protection on. In that case `OPTIMIZED_SIBLING_CHECK` must be enabled so overlap checks cover the whole catalog instead of only the parent namespace. Turning `ALLOW_TABLE_LOCATION_OVERLAP` on also satisfies the check, but disables overlap protection and is not recommended. |
| `OPTIMIZED_SIBLING_CHECK` | Realm-level only (no catalog property). Supported by the JDBC and NoSQL metastores. It relies on index data that older realms may not have, so Polaris refuses to start with it enabled unless `ALLOW_OPTIMIZED_SIBLING_CHECK` is also set; only acknowledge this for new realms. |
| `DEFAULT_UNIQUE_TABLE_LOCATION_ENABLED` | Independent option that replaces the table-name segment of generated locations with a unique, unpredictable name. It can be used with or without the object storage prefix. |

## How to enable

1. On the catalog, set these catalog properties:
   - `polaris.config.default-table-location-object-storage-prefix.enabled=true`
   - `polaris.config.allow.unstructured.table.location=true`
2. Unless you have enabled `ALLOW_TABLE_LOCATION_OVERLAP`, enable the optimized sibling check for the
   realm, for example in `application.properties`:
   ```properties
   polaris.features."OPTIMIZED_SIBLING_CHECK"=true
   polaris.features."ALLOW_OPTIMIZED_SIBLING_CHECK"=true
   ```
3. Create a new table **without** an explicit `location` and confirm that the location returned by
   Polaris includes the hash segment under the warehouse.

To enable the prefix for every catalog in a realm instead, set
`polaris.features."DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED"=true` as described in
[Configuring Polaris]({{% ref "configuring-polaris.md" %}}).

## Alternative: Iceberg `write.data.path`

When Iceberg's object store layout is enabled and a table also sets `write.data.path`, Iceberg places
the per-file hash directly under that path, followed by the namespace and table name:

```text
<write.data.path>/<file-hash>/<namespace>/<table>/...
```

Pointing `write.data.path` at a short, shared prefix such as the warehouse root therefore spreads the
data files of all tables across the key space, per file rather than per table.

Polaris treats `write.data.path` as one of the table's locations when validating allowed and
overlapping locations, so a path shared by several tables can be rejected by overlap checks. Test
this setup against your catalog's location settings before relying on it.

## See also

- [Configuring Polaris for Production]({{% ref "configuring-polaris-for-production" %}}) — general production checklist
- [Configuration reference]({{% ref "configuration-reference.md" %}}) — all feature flags and catalog properties

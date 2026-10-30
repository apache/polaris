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
title: Disabling Unstructured Table Locations
linkTitle: Disabling Unstructured Locations
type: docs
weight: 215
---

By default, Polaris requires the locations of Iceberg tables and views to be inside the location of
their namespace, in addition to the catalog's allowed locations. The realm feature
[`ALLOW_UNSTRUCTURED_TABLE_LOCATION`]({{% ref "configuration-reference.md" %}}#polarisfeaturesallow_unstructured_table_location)
and the catalog property `polaris.config.allow.unstructured.table.location` remove that
requirement when set to `true`.

This page describes what happens to **Iceberg** tables and views when the effective value of this
setting changes from `true` to `false`, and how to find and fix locations that no longer comply.
[Generic tables]({{% ref "../generic-table.md" %}}) are only checked when they are created with an
explicit base location. They are not validated on load or drop the way Iceberg tables are on commit
and credential vending.

## What is checked

When unstructured locations are disabled, Polaris checks that locations are inside the namespace
location in these cases:

- Creating or registering a table or view, and changing the location of a table or view.
- Renaming a table or view into another namespace. The locations must be inside the destination
  namespace's location.
- Committing to an existing table. The new metadata file and any other referenced locations must be
  inside both the table's namespace location and the table's own location. This applies to every
  commit, including appends.
- Vending credentials for an existing table, when a client loads the table with access delegation
  or refreshes its credentials.

A failed check returns HTTP 403 (`ForbiddenException`). Listing tables and loading a table without
vended credentials are not affected.

These checks do not apply to external catalogs, which always allow unstructured locations.

## Interaction with object storage prefix for table locations

[`DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED`]({{% ref "configuration-reference.md" %}}#polarisfeaturesdefault_location_object_storage_prefix_enabled)
(the catalog property `polaris.config.default-table-location-object-storage-prefix.enabled`)
places new Iceberg tables and views under the catalog's `default-base-location` with a hash prefix,
not under their namespace location. That layout requires unstructured locations to be enabled.

If you disable unstructured locations while the prefix flag stays enabled:

- Every existing Iceberg table or view created under the prefix layout sits outside its namespace.
  Commits and credential vending for those entities start failing with HTTP 403.
- New Iceberg tables or views created without a client-supplied location fail at create time with
  `IllegalStateException`, not HTTP 403, because Polaris refuses to generate a prefixed location
  without unstructured locations.

Turn off the prefix flag before or at the same time as disabling unstructured locations. Disabling
only unstructured locations leaves every prefixed table incompatible and blocks new default-location
creates while the prefix flag remains on.

## Changing the setting does not validate existing entities

Polaris does not scan, move, or repair table and view locations when the setting changes. The
configuration change itself always succeeds. Tables and views whose locations are inside their
namespace location keep working. Those whose locations are not inside their namespace location fail
the checks above the next time one of those operations runs.

Validating every table and view is not required before the change. You can enforce structured
locations immediately and fix incompatible entities afterwards, or find and fix them first, as
described in [Finding and fixing incompatible locations](#finding-and-fixing-incompatible-locations).

## When the effective value changes

The catalog property takes precedence over the realm feature, which takes precedence over the
default value `false`. The deprecated catalog property `allow.unstructured.table.location` is still
honored when `polaris.config.allow.unstructured.table.location` is not set. The effective value of a
catalog changes from `true` to `false` in any of these cases:

| Change | Affected catalogs |
| --- | --- |
| The catalog property is changed from `true` to `false` | That catalog |
| The catalog property is removed while the realm value is `false` | That catalog |
| The realm feature `polaris.features."ALLOW_UNSTRUCTURED_TABLE_LOCATION"` is changed from `true` to `false`, or removed | Every catalog of the realm that does not set the catalog property |

## Example: renaming a table across namespaces

1. With unstructured locations enabled, create table `ns1.t1` without a location. Polaris places it
   inside the location of `ns1`, for example `s3://bucket/warehouse/ns1/t1/`.
2. While unstructured locations are still enabled, rename the table to `ns2.t2`. The rename only
   changes the table's name and namespace: its metadata and data files stay under
   `s3://bucket/warehouse/ns1/t1/`.
3. Disable unstructured locations for the catalog.
4. Loading `ns2.t2` with vended credentials, or committing to it, now fails with HTTP 403, because
   its location is not inside the location of `ns2`.

With unstructured locations disabled, step 2 would itself fail, because renames are checked against
the destination namespace.

## Finding and fixing incompatible locations

Polaris does not report incompatible entities. To find them, compare for each table and view:

- The table or view location, and the `write.data.path` and `write.metadata.path` table properties
  if they are set, as returned when loading the table or view.
- The `location` property of its namespace. If the namespace has no `location` property, its
  location is built from the closest parent that has one (or the catalog's `default-base-location`)
  plus the names of the namespaces below that parent. For example, namespace `a.b` without a
  `location` in a catalog with `default-base-location` `s3://bucket/warehouse` is located at
  `s3://bucket/warehouse/a/b`.

Typical sources of incompatible locations are tables renamed across namespaces, tables created with
an explicit location, tables registered or migrated from another catalog, and Iceberg tables or
views created while
[`DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED`]({{% ref "configuration-reference.md" %}}#polarisfeaturesdefault_location_object_storage_prefix_enabled)
was enabled (every such table sits outside its namespace).

To fix an incompatible table or view:

- Rename it back into a namespace whose location contains its files.
- Copy the data to a location inside the namespace, for example with a `CREATE TABLE ... AS SELECT`
  statement, and drop the old table.
- Keep unstructured locations enabled for the catalog that needs them, by setting the catalog
  property to `true`.
- If the catalog uses the object storage prefix for default table locations, disable
  `polaris.config.default-table-location-object-storage-prefix.enabled` (or move tables off the
  prefixed layout) before or together with turning unstructured locations off.

If operations start failing with HTTP 403 after the change, setting the catalog property back to
`true` restores the previous behavior while you fix the affected entities.

## Related pages

- [Configuring Polaris for Production]({{% ref "configuring-polaris-for-production" %}})
- [Configuration Reference]({{% ref "configuration-reference.md" %}})

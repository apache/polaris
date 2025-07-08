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
title: Policy
type: docs
weight: 425
---

The Polaris Policy framework empowers organizations to centrally define, manage, and enforce fine-grained governance, lifecycle, and operational rules across all data resources in the catalog.

With the policy API, you can:
- Create and manage policies
- Attach policies to specific resources (catalogs, namespaces, tables, or views)
- Check applicable policies for any given resource

## What is a Policy?

A policy in Apache Polaris is a structured entity that defines rules governing actions on specified resources under
predefined conditions. Each policy contains:

- **Name**: A unique identifier within a namespace
- **Type**: Determines the semantics and expected format of the policy content
- **Description**: Explains the purpose of the policy
- **Content**: Contains the actual rules defining the policy behavior
- **Version**: An automatically tracked revision number
- **Inheritable**: Whether the policy can be inherited by child resources, decided by its type

### Policy Types

Polaris supports several predefined system policy types (prefixed with `system.`):

| Policy Type | Purpose                                               | JSON-Schema | Applies To |
|-------------|-------------------------------------------------------|-------------|------------|
| **`system.data-compaction`** | Defines rules for data file compaction operations     | [`data-compaction/2025-02-03.json`](https://polaris.apache.org/schemas/policies/system/data-compaction/2025-02-03.json) | Iceberg **table**, **namespace**, **catalog** |
| **`system.metadata-compaction`** | Defines rules for metadata file compaction operations | [`metadata-compaction/2025-02-03.json`](https://polaris.apache.org/schemas/policies/system/metadata-compaction/2025-02-03.json) | Iceberg **table**, **namespace**, **catalog** |
| **`system.orphan-file-removal`** | Defines rules for removing orphaned files             | [`orphan-file-removal/2025-02-03.json`](https://polaris.apache.org/schemas/policies/system/orphan-file-removal/2025-02-03.json) | Iceberg **table**, **namespace**, **catalog** |
| **`system.snapshot-expiry`** | Defines rules for snapshot expiration                 | [`snapshot-expiry/2025-02-03.json`](https://polaris.apache.org/schemas/policies/system/snapshot-expiry/2025-02-03.json) | Iceberg **table**, **namespace**, **catalog** |

Support for additional predefined system policy types and custom policy type definitions is in progress.
For more details, please refer to the [roadmap](https://github.com/apache/polaris/discussions/1028).

### Policy Inheritance

The entity hierarchy in Polaris is structured as follows:

```
            Catalog
               |
           Namespace
               |
   +-----------+----------+
   |           |          |
Iceberg     Iceberg     Generic
 Table       View        Table
```

Policies can be attached at any level, and inheritance flows from catalog down to namespace, then to tables and views.

Policies can be inheritable or non-inheritable:

- **Inheritable policies**: Apply to the target resource and all its applicable child resources
- **Non-inheritable policies**: Apply only to the specific target resource

The inheritance follows an override mechanism:
1. Table-level policies override namespace and catalog policies
2. Namespace-level policies override parent namespace and catalog policies

> [!IMPORTANT]
> Because an override completely replaces the same policy type at higher levels,
> **only one instance of a given policy type can be attached to (and therefore affect) a resource**.

## Working with Policies

### Creating a Policy

To create a policy, you need to provide a name, type, and optionally a description and content:

```json
POST /polaris/v1/{prefix}/namespaces/{namespace}/policies
{
  "name": "compaction-policy",
  "type": "system.data-compaction",
  "description": "Policy for optimizing table storage",
  "content": "{\"version\": \"2025-02-03\", \"enable\": true, \"config\": {\"target_file_size_bytes\": 134217728}}"
}
```

The policy content is validated against a schema specific to its type. Here are a few policy content examples:
- Data Compaction Policy
```json
{
  "version": "2025-02-03",
  "enable": true,
  "config": {
    "target_file_size_bytes": 134217728,
    "compaction_strategy": "bin-pack",
    "max-concurrent-file-group-rewrites": 5
  }
}
```
- Orphan File Removal Policy
```json
{
  "version": "2025-02-03",
  "enable": true,
  "max_orphan_file_age_in_days": 30,
  "locations": ["s3://my-bucket/my-table-location"],
  "config": {
    "prefix_mismatch_mode": "ignore"
  }
}
```

### Attaching Policies to Resources

Policies can be attached to different resource levels:

1. **Catalog level**: Applies to the entire catalog
2. **Namespace level**: Applies to a specific namespace
3. **Table-like level**: Applies to individual tables or views

Example of attaching a policy to a table:

```json
PUT /polaris/v1/{prefix}/namespaces/{namespace}/policies/{policy-name}/mappings
{
  "target": {
    "type": "table-like",
    "path": ["NS1", "NS2", "test_table_1"]
  }
}
```

For inheritable policies, only one policy of a given type can be attached to a resource. For non-inheritable policies,
multiple policies of the same type can be attached.

### Retrieving Applicable Policies
A user can view applicable policies on a resource (e.g., table, namespace, or catalog) as long as they have
read permission on that resource.

Here is an example to find all policies that apply to a specific resource (including inherited policies):
```
GET /polaris/v1/catalog/applicable-policies?namespace=finance%1Fquarterly&target-name=transactions
```

**Sample response:**
```json
{
  "policies": [
    {
      "name": "snapshot-expiry-policy",
      "type": "system.snapshot-expiry",
      "appliedAt": "namespace",
      "content": {
        "version": "2025-02-03",
        "enable": true,
        "config": {
          "min_snapshot_to_keep": 1,
          "max_snapshot_age_days": 2,
          "max_ref_age_days": 3
        }
      }
    },
    {
      "name": "compaction-policy",
      "type": "system.data-compaction",
      "appliedAt": "catalog",
      "content": {
        "version": "2025-02-03",
        "enable": true,
        "config": {
          "target_file_size_bytes": 134217728
        }
      }
    }
  ]
}
```

### API Reference

For the complete and up-to-date API specification, see the [policy-api.yaml](https://github.com/apache/polaris/blob/main/spec/polaris-catalog-apis/policy-apis.yaml).

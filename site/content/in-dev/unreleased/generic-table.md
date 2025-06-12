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
title: Generic Table (Beta Version)
type: docs
weight: 435
---

The Generic Table in Apache Polaris provides basic management support for non-Iceberg tables. 

With the Generic Table API, you can:
- Create generic tables under a namespace
- Load a generic table 
- Drop a generic table
- List all generic table under a namespace

**NOTE** The current generic table support is beta release, there could potentially be large changes 

## What is a Generic Table?

A generic table in Apache Polaris is a structured entity that defines the necessary information. Each 
generic table entity contains the following properties:

- **name** (required): A unique identifier for the table within a namespace
- **format** (required): The format for the generic table, i.e. "delta", "csv"
- **base-location** (optional): Table base location in URI format. For example: s3://<my-bucket>/path/to/table
  - The table base location is a location that includes all files for the table
  - A table with multiple disjoint locations (i.e. containing files that are outside the configured base location) is not compliant with the current generic table support in Polaris.
  - If no location is provided, clients or users are responsible for managing the location.
- **properties** (optional): Properties for the generic table passed on creation
- **doc** (optional): Comment or description for the table

## Generic Table API Vs. Iceberg Table API
| Operations   | **Generic Table API**        | **Iceberg Table API**                                           |
|--------------|------------------------------| --------------------------------------------------------------- |
| Create Table | Api                          | Iceberg-specific interface for working with Iceberg tables      |
| **Scope**    | Abstract and format-agnostic | Tightly coupled with Apache Iceberg format                      |


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
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
title: Generic Table (Beta)
type: docs
weight: 435
---

The Generic Table in Apache Polaris is designed to provide support for non-Iceberg tables across different table formats includes delta, csv etc. It currently provides the following capabilities:
- Create a generic table under a namespace
- Load a generic table
- Drop a generic table
- List all generic tables under a namespace

**NOTE** The current generic table is in beta release. Please use it with caution and report any issue if encountered.

## What is a Generic Table?

A generic table in Polaris is an entity that defines the following fields:

- **name** (required): A unique identifier for the table within a namespace
- **format** (required): The format for the generic table, i.e. "delta", "csv"
- **base-location** (optional): Table base location in URI format. For example: s3://<my-bucket>/path/to/table
  - The table base location is a location that includes all files for the table
  - A table with multiple disjoint locations (i.e. containing files that are outside the configured base location) is not compliant with the current generic table support in Polaris.
  - If no location is provided, clients or users are responsible for managing the location.
- **properties** (optional): Properties for the generic table passed on creation.
  - Currently, there is no reserved property key defined.
  - The property definition and interpretation is delegated to client or engine implementations.
- **doc** (optional): Comment or description for the table

## Generic Table API Vs. Iceberg Table API

Generic Table provides a different set of APIs to operate on the generic table entities while Iceberg APIs operates on
the Iceberg table entities.

| Operations   | **Iceberg Table API**                                                                                                                                               | **Generic Table API**                                                                                                         |
|--------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| Create Table | Create an Iceberg table                                                                                                                                             | Create a generic table                                                                                                        |
| Load Table   | Load an Iceberg table. If the table to load is a generic table, you need to call the Generic Table loadTable API, otherwise a TableNotFoundException will be thrown | Load a generic table. Similarly, try to load an Iceberg table through Generic Table API will thrown a TableNotFoundException. |
| Drop Table   | Drop an Iceberg table. Similar as load table, if the table to drop is a Generic table, a tableNotFoundException will be thrown.                                     | Drop a generic table. Drop an Iceberg table through Generic table endpoint will thrown an TableNotFound Exception             |
| List Table   | List all Iceberg tables                                                                                                                                             | List all generic tables                                                                                                       |

Note that generic table shares the same namespace with Iceberg tables, the table name has to be unique under the same namespace. Furthermore, since
there is currently no support for Update Generic Table, any update to the existing table requires a drop and re-create.

## Working with Generic Table

There are two ways to work with Polaris Generic Tables today:
1) Directly communicate with Polaris through REST API calls using tools such as `curl`. Details will be described in the later section.
2) Use the Spark client provided if you are working with Spark. Please refer to [Polaris Spark Client]({{% ref "polaris-spark-client" %}}) for detailed instructions.

### Create a Generic Table

To create a generic table, you need to provide the corresponding fields as described in [What is a Generic Table](#what-is-a-generic-table).

The REST API for creating a generic Table is `POST /polaris/v1/{prefix}/namespaces/{namespace}/generic-tables`, and the
request body looks like the following:

```json
{
  "name": "<table_name>",
  "format": "<table_format>",
  "base-location": "<table_base_location>",
  "doc": "<comment or description for table>",
  "properties": {
    "<property-key>": "<property-value>"
  }
}
```

Here is an example to create a generic table with name `delta_table` and format as `delta` under a namespace `delta_ns`
for catalog `delta_catalog` using curl:

```shell
curl -X POST http://localhost:8181/api/catalog/polaris/v1/delta_catalog/namespaces/delta_ns/generic-tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "delta_table",
    "format": "delta",
    "base-location": "s3://<my-bucket>/path/to/table",
    "doc": "delta table example",
    "properties": {
      "key1": "value1"
    }
  }'
```

### Load a Generic Table
The REST endpoint for load a generic table is `GET /polaris/v1/{prefix}/namespaces/{namespace}/generic-tables/{generic-table}`.

Here is an example to load the table `delta_table` using curl:
```shell
curl -X GET http://localhost:8181/api/catalog/polaris/v1/delta_catalog/namespaces/delta_ns/generic-tables/delta_table
```
And the response looks like the following:
```json
{
  "table": {
    "name": "delta_table",
    "format": "delta",
    "base-location": "s3://<my-bucket>/path/to/table",
    "doc": "delta table example",
    "properties": {
      "key1": "value1"
    }
  }
}
```

### List Generic Tables
The REST endpoint for listing the generic tables under a given
namespace is `GET /polaris/v1/{prefix}/namespaces/{namespace}/generic-tables/`.

Following curl command lists all tables under namespace delta_namespace:
```shell
curl -X GET http://localhost:8181/api/catalog/polaris/v1/delta_catalog/namespaces/delta_ns/generic-tables/
```
Example Response:
```json
{
  "identifiers": [
    {
      "namespace": ["delta_ns"],
      "name": "delta_table"
    }
  ],
  "next-page-token": null
}
```

### Drop a Generic Table
The drop generic table REST endpoint is `DELETE /polaris/v1/{prefix}/namespaces/{namespace}/generic-tables/{generic-table}`

The following curl call drops the table `delat_table`:
```shell
curl -X DELETE http://localhost:8181/api/catalog/polaris/v1/delta_catalog/namespaces/delta_ns/generic-tables/{generic-table}
```

### API Reference

For the complete and up-to-date API specification, see the [Catalog API Spec](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/polaris/refs/heads/main/spec/generated/bundled-polaris-catalog-service.yaml).

## Limitations

Current limitations of Generic Table support:
1) Limited spec information. Currently, there is no spec for information like Schema, Partition etc.
2) No commit coordination or update capability provided at the catalog service level.

Therefore, the catalog itself is unaware of anything about the underlying table except some of the loosely defined metadata.
It is the responsibility of the engine (and plugins used by the engine) to determine exactly how loading or commiting data
should look like based on the metadata. For example, with the delta support, th delta log serialization, deserialization
and update all happens at client side.

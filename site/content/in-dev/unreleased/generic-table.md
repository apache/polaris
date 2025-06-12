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

The Generic Table in Apache Polaris provides basic management support for non-Iceberg tables. 

With the Generic Table API, you can:
- Create generic tables under a namespace
- Load a generic table 
- Drop a generic table
- List all generic tables under a namespace

**NOTE** The current generic table is in beta release, there can still be incomplete features and bugs. Please use it
with caution and report any issue if encountered.

## What is a Generic Table?

A generic table in Polaris is a structured entity that defines the necessary information. Each 
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

Generic Table provides a different set of APIs to operate on the Generic Table entities while Iceberg APIs operates on
the Iceberg Table entities.

| Operations   | **Generic Table API**                                       | **Iceberg Table API**                                       |
|--------------|-------------------------------------------------------------|-------------------------------------------------------------|
| Create Table | create an Iceberg table                                     | create a generic table                                      |
| Load Table   | load an Iceberg table, load a generic table throws an error | load a generic table, load an Iceberg table throws an error |
| Drop Table   | drop an Iceberg table, drop a generic table throws an error | drop a generic table, drop an Iceberg table throws an error |
| List Table   | list all Iceberg tables                                     | list all generic tables                                     |

Note that generic table shares the same namespace with Iceberg tables, the table name has to be unique under the same namespace.

## Working with Generic Table

There are two ways to work with Polaris Generic Tables today:
1) Directly communicate with Polaris through REST API calls using curl. Details will be described in the later section.
2) Use the Spark Client provided if you are working with Spark. Please refer to [Polaris Spark Client]({{% ref "polaris-spark-client" %}}) for detailed instructions.

### Create a Generic Table

To create a generic table, you need to provide the corresponding fields as described in [What is a Generic Table](#what-is-a-generic-table).
Following is the REST API for creating a generic Table:

```json
POST /polaris/v1/{prefix}/namespaces/{namespace}/generic-tables
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
for catalog `delta_catalog`:

```json
POST /polaris/v1/delta_catalog/namespaces/delta_ns/generic-tables
{
  "name": "delta_table",
  "format": "delta",
}
```

### Load a Generic Table
The REST API for load a generic table is the following:

```json
GET /polaris/v1/{prefix}/namespaces/{namespace}/generic-tables/{generic-table}
```

Here is an example to load the table `delta_table`:
```json
GET /polaris/v1/delta_catalog/namespaces/delta_ns/generic-tables/{generic-table}
```
And the response looks like the following:
```json
{
  "table": {
    "name": "delta_table",
    "format": "delta",
    "base-location": null,
    "doc": null,
    "properties": null
  }
}
```

### List a Generic Table
Here is the REST API for listing the generic tables under a given namespace:
```json
GET /polaris/v1/{prefix}/namespaces/{namespace}/generic-tables/
```

Following rest call lists all tables under namespace delta_namespace:
```json
GET /polaris/v1/delta_catalog/namespaces/delta_ns/generic-tables/
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
The drop generic table REST API is the following:
```json
DELETE /polaris/v1/{prefix}/namespaces/{namespace}/generic-tables/{generic-table}
```

To drop the table `delat_table`, use the following:
```json
DELETE /polaris/v1/delta_catalog/namespaces//generic-tables/{generic-table}
```

### API Reference

For the complete and up-to-date API specification, see the [generic-tables-api.yaml](https://github.com/apache/polaris/blob/main/spec/polaris-catalog-apis/generic-tables-api.yaml).


## Limitations

The support for cross engine sharing of Generic Table is very limited:
1) Limited spec information. Currently, there is no spec for information like Schema, Partition etc. 
2) No commit coordination or update capability provided at the catalog service level.

Today, all those responsibility will be at the client side. For example, the Delta log serialization, deserialization and update are all done at Client side.
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
linkTitle: 'In Development'
title: 'Overview'
type: docs
weight: 200
params:
  top_hidden: true
  show_page_toc: false
cascade:
  type: docs
  params:
    show_page_toc: true
# This file will NOT be copied into a new release's versioned docs folder.
---

> [!WARNING]
> These pages refer to the current state of the main branch, which is still under active development.
>
> Functionalities can be changed, removed or added without prior notice.

Apache Polaris (Incubating) is a catalog implementation for Apache Iceberg&trade; tables and is built on the open source Apache Iceberg&trade; REST protocol.

With Polaris, you can provide centralized, secure read and write access to your Iceberg tables across different REST-compatible query engines.

![Conceptual diagram of Apache Polaris (Incubating).](/img/overview.svg "Apache Polaris (Incubating) overview")

## Key concepts

This section introduces key concepts associated with using Apache Polaris (Incubating).

In the following diagram, a sample [Apache Polaris (Incubating) structure](#catalog) with nested [namespaces](#namespace) is shown for Catalog1. No tables
or namespaces have been created yet for Catalog2 or Catalog3.

![Diagram that shows an example Apache Polaris (Incubating) structure.](/img/sample-catalog-structure.svg "Sample Apache Polaris (Incubating) structure")

### Catalog

In Polaris, you can create one or more catalog resources to organize Iceberg tables.

Configure your catalog by setting values in the storage configuration for S3, Azure, or Google Cloud Storage. An Iceberg catalog enables a
query engine to manage and organize tables. The catalog forms the first architectural layer in the [Apache Iceberg&trade; table specification](https://iceberg.apache.org/spec/#overview) and must support the following tasks:

- Storing the current metadata pointer for one or more Iceberg tables. A metadata pointer maps a table name to the location of that table's
  current metadata file.

- Performing atomic operations so that you can update the current metadata pointer for a table to the metadata pointer of a new version of
  the table.

To learn more about Iceberg catalogs, see the [Apache Iceberg&trade; documentation](https://iceberg.apache.org/concepts/catalog/).

#### Catalog types

A catalog can be one of the following two types:

- Internal: The catalog is managed by Polaris. Tables from this catalog can be read and written in Polaris.

- External: The catalog is externally managed by another Iceberg catalog provider (for example, Snowflake, Glue, Dremio Arctic). Tables from
  this catalog are synced to Polaris. These tables are read-only in Polaris.

A catalog is configured with a storage configuration that can point to S3, Azure storage, or GCS.

### Namespace

You create *namespaces* to logically group Iceberg tables within a catalog. A catalog can have multiple namespaces. You can also create
nested namespaces. Iceberg tables belong to namespaces.

> [!Important]
> For the access privileges defined for a catalog to be enforced correctly, the following conditions must be met:
>
> - The directory only contains the data files that belong to a single table.
> - The directory hierarchy matches the namespace hierarchy for the catalog.
>
> For example, if a catalog includes the following items:
>
> - Top-level namespace namespace1
> - Nested namespace namespace1a
> - A customers table, which is grouped under nested namespace namespace1a
> - An orders table, which is grouped under nested namespace namespace1a
>
> The directory hierarchy for the catalog must follow this structure:
>
> - /namespace1/namespace1a/customers/<files for the customers table *only*>
> - /namespace1/namespace1a/orders/<files for the orders table *only*>

### Storage configuration

A storage configuration stores a generated identity and access management (IAM) entity for your cloud storage and is created
when you create a catalog. The storage configuration is used to set the values to connect Polaris to your cloud storage. During the
catalog creation process, an IAM entity is generated and used to create a trust relationship between the cloud storage provider and Polaris
Catalog.

When you create a catalog, you supply the following information about your cloud storage:

| Cloud storage provider | Information |
| -----------------------| ----------- |
| Amazon S3 | <ul><li>Default base location for your Amazon S3 bucket</li><li>Locations for your Amazon S3 bucket</li><li>S3 role ARN</li><li>External ID (optional)</li></ul> |
| Google Cloud Storage (GCS) | <ul><li>Default base location for your GCS bucket</li><li>Locations for your GCS bucket</li></ul> |
| Azure | <ul><li>Default base location for your Microsoft Azure container</li><li>Locations for your Microsoft Azure container</li><li>Azure tenant ID</li></ul> |

## Example workflow

In the following example workflow, Bob creates an Apache Iceberg&trade; table named Table1 and Alice reads data from Table1.

1.  Bob uses Apache Spark&trade; to create the Table1 table under the
    Namespace1 namespace in the Catalog1 catalog and insert values into
    Table1.

    Bob can create Table1 and insert data into it because he is using a
    service connection with a service principal that has
    the privileges to perform these actions.

2.  Alice uses Snowflake to read data from Table1.

    Alice can read data from Table1 because she is using a service
    connection with a service principal with a catalog integration that
    has the privileges to perform this action. Alice
    creates an unmanaged table in Snowflake to read data from Table1.

![Diagram that shows an example workflow for Apache Polaris (Incubating)](/img/example-workflow.svg "Example workflow for Apache Polaris (Incubating)")

## Security and access control

### Credential vending

To secure interactions with service connections, Polaris vends temporary storage credentials to the query engine during query
execution. These credentials allow the query engine to run the query without requiring access to your cloud storage for
Iceberg tables. This process is called credential vending.

As of now, the following limitation is known regarding Apache Iceberg support:

- **remove_orphan_files:** Apache Spark can't use credential vending
  for this due to a known issue. See [apache/iceberg#7914](https://github.com/apache/iceberg/pull/7914) for details.

### Identity and access management (IAM)

Polaris uses the identity and access management (IAM) entity to securely connect to your storage for accessing table data, Iceberg
metadata, and manifest files that store the table schema, partitions, and other metadata. Polaris retains the IAM entity for your
storage location.

### Access control

Polaris enforces the access control that you configure across all tables registered with the service and governs security for all
queries from query engines in a consistent manner.

Polaris uses a role-based access control (RBAC) model that lets you centrally configure access for Polaris service principals to catalogs,
namespaces, and tables.

Polaris RBAC uses two different role types to delegate privileges:

- **Principal roles:** Granted to Polaris service principals and
  analogous to roles in other access control systems that you grant to
  service principals.

- **Catalog roles:** Configured with certain privileges on Polaris
  catalog resources and granted to principal roles.

For more information, see [Access control]({{% ref "access-control" %}}).

## Legal Notices

Apache&reg;, Apache Iceberg&trade;, Apache Spark&trade;, Apache Flink&reg;, and Flink&reg; are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.


<!--
Testing the `releaseVersion` shortcode here: version is: {{< releaseVersion >}}
-->

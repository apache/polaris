<!--

 Copyright (c) 2024 Snowflake Computing Inc.
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

-->

Polaris Catalog is a catalog implementation for Apache Iceberg built on the open source Apache Iceberg REST protocol.

With Polaris Catalog, you can provide centralized, secure read and write access across different REST-compatible query engines to your Iceberg tables.

![Conceptual diagram of Polaris Catalog.](./img/overview.svg "Polaris Catalog overview")

## Key concepts

This section introduces key concepts associated with using Polaris Catalog.

In the following diagram, a sample [Polaris Catalog structure](./overview.md#catalog) with nested [namespaces](./overview.md#namespace) is shown for Catalog1. No tables
or namespaces have been created yet for Catalog2 or Catalog3:

![Diagram that shows an example Polaris Catalog structure.](./img/sample-catalog-structure.svg "Sample Polaris Catalog structure")

### Catalog

In Polaris Catalog, you can create one or more catalog resources to organize Iceberg tables.

Configure your catalog by setting values in the storage configuration for S3, Azure, or Google Cloud Storage. An Iceberg catalog enables a
query engine to manage and organize tables. The catalog forms the first architectural layer in the [Iceberg table specification](https://iceberg.apache.org/spec/#overview) and must support:

- Storing the current metadata pointer for one or more Iceberg tables. A metadata pointer maps a table name to the location of that table's current metadata file.
- Performing atomic operations so that you can update the current metadata pointer for a table to the metadata pointer of a new version of the table.

To learn more about Iceberg catalogs, see the [Apache Iceberg documentation](https://iceberg.apache.org/concepts/catalog/).

#### Catalog types

A catalog can be one of the following two types:

- Internal: The catalog is managed by Polaris. Tables from this catalog can be read and written in Polaris.
- External: The catalog is externally managed by another Iceberg catalog provider (for example, Snowflake, Glue, Dremio Arctic). Tables from
  this catalog are synced to Polaris. These tables are read-only in Polaris. In the current release, only Snowflake external catalog is provided.

A catalog is configured with a storage configuration that can point to S3, Azure storage, or GCS.

To create a new catalog, see [Create a catalog](./create-a-catalog.md "Sample Polaris Catalog structure").

### Namespace

You create *namespaces* to logically group Iceberg tables within a catalog. A catalog can have one or more namespaces. You can also create
nested namespaces. Iceberg tables belong to namespaces.

### Iceberg tables & catalogs

In an internal catalog, an Iceberg table is registered in Polaris Catalog, but read and written via query engines. The table data and
metadata is stored in your external cloud storage. The table uses Polaris Catalog as the Iceberg catalog.

If you have tables that use Snowflake as the Iceberg catalog (Snowflake-managed tables), you can sync these tables to an external
catalog in Polaris Catalog. If you sync this catalog to Polaris Catalog, it appears as an external catalog in Polaris Catalog. The table data and
metadata is stored in your external cloud storage. The Snowflake query engine can read from or write to these tables. However, the other query
engines can only read from these tables.

**Important**

To ensure that the access privileges defined for a catalog are enforced
correctly, you must:

- Ensure a directory only contains the data files that belong to a single table.
- Create a directory hierarchy that matches the namespace hierarchy for the catalog.

For example, if a catalog includes:

- Top-level namespace namespace1
- Nested namespace namespace1a
- A customers table, which is grouped under nested namespace namespace1a
- An orders table, which is grouped under nested namespace namespace1a

The directory hierarchy for the catalog must be:

- /namespace1/namespace1a/customers/\<files for the customers table *only*>
- /namespace1/namespace1a/orders/\<files for the orders table *only*>

### Service principal

A service principal is an entity that you create in Polaris Catalog. Each service principal encapsulates credentials that you use to connect to Polaris Catalog.

Query engines use service principals to connect to catalogs.

Polaris Catalog generates a Client ID and Client Secret pair for each service principal.

The following table displays example service principals that you might create in Polaris Catalog:

| Service connection name  | Description                                                                  |
|--------------------------|------------------------------------------------------------------------------|
| Flink ingestion          | For Apache Flink to ingest streaming data into Iceberg tables.               |
| Spark ETL pipeline       | For Apache Spark to run ETL pipeline jobs on Iceberg tables.                 |
| Snowflake data pipelines | For Snowflake to run data pipelines for transforming data in Iceberg tables. |
| Trino BI dashboard       | For Trino to run BI queries for powering a dashboard.                        |
| Snowflake AI team        | For Snowflake to run AI jobs on data in Iceberg tables.                      |

### Service connection

A service connection represents a REST-compatible engine (such as Apache Spark, Apache Flink, or Trino) that can read from and write to Polaris
Catalog. When creating a new service connection, the Polaris administrator grants the service principal that is created with the new service
connection with either a new or existing principal role. A principal role is a resource in Polaris that you can use to logically group Polaris
service principals together and grant privileges on securable objects. For more information, see [Principal role](./access-control.md#principal-role "Principal role"). Polaris Catalog uses a role-based access control (RBAC) model to grant service principals access to resources. For more information,
see [Access control](./access-control.md "Access control"). For a diagram of this model, see [RBAC model](./access-control.md#rbac-model "RBAC model").

If the Polaris administrator grants the service principal for the new service connection with a new principal role, the service principal
doesn't have any privileges granted to it yet. When securing the catalog that the new service connection will connect to, the Polaris
administrator grants privileges to catalog roles and then grants these catalog roles to the new principal role. As a result, the service
principal for the new service connection is bestowed with these privileges. For more information about catalog roles, see [Catalog role](./access-control.md#catalog-role "Catalog role").

If the Polaris administrator grants an existing principal role to the service principal for the new service connection, the service principal
is bestowed with the privileges granted to the catalog roles that are granted to the existing principal role. If needed, the Polaris
administrator can grant additional catalog roles to the existing principal role or remove catalog roles from it to adjust the privileges
bestowed to the service principal. For an example of how RBAC works in Polaris, see [RBAC example](./access-control.md#rbac-example "RBAC example").

### Storage configuration

A storage configuration stores a generated identity and access management (IAM) entity for your external cloud storage and is created
when you create a catalog. The storage configuration is used to set the values to connect Polaris Catalog to your cloud storage. During the
catalog creation process, an IAM entity is generated and used to create a trust relationship between the cloud storage provider and Polaris
Catalog.

When you create a catalog, you supply the following information about your external cloud storage:

| Cloud storage provider     | Information                                                                                                                                                      |
|----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Amazon S3                  | <ul><li>Default base location for your Amazon S3 bucket</li><li>Locations for your Amazon S3 bucket</li><li>S3 role ARN</li><li>External ID (optional)</li></ul> |
| Google Cloud Storage (GCS) | <ul><li>Default base location for your GCS bucket</li><li>Locations for your Amazon GCS bucket</li></ul>                                                         |
| Azure                      | <ul><li>Default base location for your Microsoft Azure container</li><li>Locations for your Microsoft Azure container</li><li>Azure tenant ID</li></ul>          |

## Example workflow

In the following example workflow, Bob creates an Iceberg table named Table1 and Alice reads data from Table1.

1. Bob uses Apache Spark to create the Table1 table under the
   Namespace1 namespace in the Catalog1 catalog and insert values into
   Table1.

   Bob can create Table1 and insert data into it, because he is using a
   service connection with a service principal that is bestowed with
   the privileges to perform these actions.

2. Alice uses Snowflake to read data from Table1.

   Alice can read data from Table1, because she is using a service
   connection with a service principal with a catalog integration that
   is bestowed with the privileges to perform this action. Alice
   creates an unmanaged table in Snowflake to read data from Table1.

![Diagram that shows an example workflow for Polaris Catalog](./img/example-workflow.svg "Example workflow for Polaris Catalog")

## Security and access control

This section describes security and access control.

### Credential vending

To secure interactions with service connections, Polaris Catalog vends temporary storage credentials to the query engine during query
execution. These credentials allow the query engine to run the query without needing to have access to your external cloud storage for
Iceberg tables. This process is called credential vending.

### Identity and access management (IAM)

Polaris Catalog uses the identity and access management (IAM) entity to securely connect to your storage for accessing table data, Iceberg
metadata, and manifest files that store the table schema, partitions, and other metadata. Polaris Catalog retains the IAM entity for your
storage location.

### Access control

Polaris Catalog enforces the access control that you configure across all tables registered with the service, and governs security for all
queries from query engines in a consistent manner.

Polaris uses a role-based access control (RBAC) model that lets you centrally configure access for Polaris service principals to catalogs,
namespaces, and tables.

Polaris RBAC uses two different role types to delegate privileges:

- **Principal roles:** Granted to Polaris service principals and
  analogous to roles in other access control systems that you grant to
  service principals.

- **Catalog roles:** Configured with certain privileges on Polaris
  catalog resources, and granted to principal roles.

For more information, see [Access control](./access-control.md "Access control").

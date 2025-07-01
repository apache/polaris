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
Title: Entities
type: docs
weight: 400
---

This page documents various entities that can be managed in Apache Polaris (Incubating).

## Catalog

A catalog is a top-level entity in Polaris that may contain other entities like [namespaces](#namespace) and [tables](#table). These map directly to [Apache Iceberg catalogs](https://iceberg.apache.org/concepts/catalog/).

For information on managing catalogs with the REST API or for more information on what data can be associated with a catalog, see [the API docs]({{% github-polaris "client/python/docs/CreateCatalogRequest.md" %}}).

### Storage Type

All catalogs in Polaris are associated with a _storage type_. Valid Storage Types are `S3`, `Azure`, and `GCS`. The `FILE` type is also additionally available for testing. Each of these types relates to a different storage provider where data within the catalog may reside. Depending on the storage type, various other configurations may be set for a catalog including credentials to be used when accessing data inside the catalog.

For details on how to use Storage Types in the REST API, see [the API docs]({{% github-polaris "client/python/docs/StorageConfigInfo.md" %}}).

For usage examples of storage types, see [docs]({{% ref "command-line-interface" %}}).

## Namespace

A namespace is a logical entity that resides within a [catalog](#catalog) and can contain other entities such as [tables](#table) or [views](#view). Some other systems may refer to namespaces as _schemas_ or _databases_.

In Polaris, namespaces can be nested. For example, `a.b.c.d.e.f.g` is a valid namespace. `b` is said to reside within `a`, and so on.

For information on managing namespaces with the REST API or for more information on what data can be associated with a namespace, see [the API docs]({{% github-polaris "client/python/docs/CreateNamespaceRequest.md" %}}).

## Table

Polaris tables are entities that map to [Apache Iceberg tables](https://iceberg.apache.org/docs/nightly/configuration/), [Delta tables](https://docs.databricks.com/aws/en/delta/table-properties), or [Hudi tables](https://hudi.apache.org/docs/next/configurations#TABLE_CONFIG).

For information on managing tables with the REST API or for more information on what data can be associated with a table, see [the API docs]({{% github-polaris "client/python/docs/CreateTableRequest.md" %}}).

## View

Polaris views are entities that map to [Apache Iceberg views](https://iceberg.apache.org/view-spec/).

For information on managing views with the REST API or for more information on what data can be associated with a view, see [the API docs]({{% github-polaris "client/python/docs/CreateViewRequest.md" %}}).

## Principal

Polaris principals are unique identities that can be used to represent users or services. Each principal may have one or more [principal roles](#principal-role) assigned to it for the purpose of accessing catalogs and the entities within them.

For information on managing principals with the REST API or for more information on what data can be associated with a principal, see [the API docs]({{% github-polaris "client/python/docs/CreatePrincipalRequest.md" %}}).

## Principal Role

Polaris principal roles are labels that may be granted to [principals](#principal). Each principal may have one or more principal roles, and the same principal role may be granted to multiple principals. Principal roles may be assigned based on the persona or responsibilities of a given principal, or on how that principal will need to access different entities within Polaris.

For information on managing principal roles with the REST API or for more information on what data can be associated with a principal role, see [the API docs]({{% github-polaris "client/python/docs/CreatePrincipalRoleRequest.md" %}}).

## Catalog Role

Polaris catalog roles are labels that may be granted to [catalogs](#catalog). Each catalog may have one or more catalog roles, and the same catalog role may be granted to multiple catalogs. Catalog roles may be assigned based on the nature of data that will reside in a catalog, or by the groups of users and services that might need to access that data.

Each catalog role may have multiple [privileges](#privilege) granted to it, and each catalog role can be granted to one or more [principal roles](#principal-role). This is the mechanism by which principals are granted access to entities inside a catalog such as namespaces and tables.

## Policy

Polaris policy is a set of rules governing actions on specified resources under predefined conditions. Polaris support policy for Iceberg table compaction, snapshot expiry, row-level access control, and custom policy definitions.

Policy can be applied at catalog level, namespace level, or table level. Policy inheritance can be achieved by attaching one to a higher-level scope, such as namespace or catalog. As a result, tables registered under those entities do not need to be declared individually for the same policy. If a table or a namespace requires a different policy, user can assign a different policy, hence overriding policy of the same type declared at the higher level entities.

## Privilege

Polaris privileges are granted to [catalog roles](#catalog-role) in order to grant principals with a given principal role some degree of access to catalogs with a given catalog role. When a privilege is granted to a catalog role, any principal roles granted that catalog role receive the privilege. In turn, any principals who are granted that principal role receive it.

A privilege can be scoped to any entity inside a catalog, including the catalog itself.

For a list of supported privileges for each privilege class, see the API docs:
* [Table Privileges]({{% github-polaris "client/python/docs/TablePrivilege.md" %}})
* [View Privileges]({{% github-polaris "client/python/docs/ViewPrivilege.md" %}})
* [Namespace Privileges]({{% github-polaris "client/python/docs/NamespacePrivilege.md" %}})
* [Catalog Privileges]({{% github-polaris "client/python/docs/CatalogPrivilege.md" %}})

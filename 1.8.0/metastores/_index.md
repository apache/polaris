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
title: Metastores
type: docs
weight: 700
---


A metastore is the persistence backend that stores and manages catalog metadata. This includes:

- Polaris [entities](../entities): catalogs, namespaces, tables, views, principals, roles, etc.
- Polaris [RBAC data](../managing-security/access-control): authorization and access control information (when using the built-in authorizer).
- Polaris [policies](../policy): policy configurations for data governance and security.

Polaris supports three metastore implementations and persistence backends:

| Backend           | Type              | Support Level | Description                                                                                     |
|-------------------|-------------------|---------------|-------------------------------------------------------------------------------------------------|
| In-Memory         | `in-memory`       | Test only     | Data is stored in memory and lost when pods restart. Suitable for development and testing only. |
| PostgreSQL (JDBC) | `relational-jdbc` | Ready to use  | Data is stored in a PostgreSQL database using JDBC. Recommended for production.                 |
| MongoDB (NoSQL)   | `nosql`           | Experimental  | Data is stored in a MongoDB database. Currently in beta.                                        |

{{< alert warning >}}
The default `in-memory` backend is **not suitable for production**. Data will be lost when the server restarts!
{{< /alert >}}

This section explains how to configure and use Polaris with the following backends:

- [Relational JDBC](relational-jdbc)
- [NoSQL MongoDB](nosql-mongodb)

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
Title: Realm
type: docs
weight: 350
---

This page explains what a realm is and what it is used for in Polaris.

### What is it?

A realm in Polaris serves as logical partitioning mechanism within the catalog system. This isolation allows for multitenancy, enabling different teams, environments or organizations to operate independently within the same Polaris deployment.

### Key Characteristics

**Isolation:** Each realm encapsulates its own set of resources, ensuring that operations, policies in one realm do not affect others.

**Authentication Context:** When configuring Polaris, principals credentials are associated with a specific realm. This allows for the separation of security concerns across different realms.

**Configuration Scope:** Realm identifiers are used in various configurations, such as connection strings feature configurations, etc.

An example of this is:

`jdbc:postgresql://localhost:5432/{realm}`

This ensures that each realm's data is stored separately.

### How is it used in the system?

**RealmContext:**  It is a key concept used to identify and resolve the context in which operations are performed. For example `DefaultRealmContextResolver`, a realm is resolved from request headers, and operations are performed based on the resolved realm identifier.

**Authentication and Authorization:** For example, in `BasePolarisAuthenticator`, `RealmContext` is used to provide context about the current security domain, which is used to retrieve the correct `PolarisMetastoreManager` that manages all Polaris entities and associated grant records metadata for
authorization.

**Isolation:** In methods like `createEntityManagerFactory(@Nonnull RealmContext realmContext)` from `PolarisEclipseLinkPersistenceUnit` interface, the realm context influence how resources are created or managed based on the security policies of that realm.
An example of this is the way a realm name can be used to create a database connection url so that you have one database instance per realm, when applicable. Or it can be more granular and applied at primary key level (within the same database instance).

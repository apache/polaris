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
title: Polaris Evolution
type: docs
weight: 1000
---

This page discusses what can be expected from Apache Polaris as the project evolves.

## Using Polaris as a Catalog

Polaris is primarily intended to be used as a Catalog of Tables and Views. As such, 
it implements the Iceberg REST API, Polaris Management API and the Generic Tables API.

Revisions of the Iceberg REST API are controlled by the [Apache Iceberg](https://iceberg.apache.org/)
community. Polaris attempts to accurately implement this specification. Nonetheless,
optional REST Catalog features may or may not be supported immediately. In general,
there is no guarantee that Polaris releases always implement the latest version of
the Iceberg REST Catalog API.

Any API under Polaris control (e.g. the Management API) is maintained as a versioned REST
API. New releases of Polaris may include changes to the current version of the API. When
that happens those changes are intended to be compatible with prior versions of Polaris 
clients. Certain endpoints and parameters may be deprecated.

In case a major change is required to the Management API that cannot be implemented in a
backward-compatible way, new endpoints (URI paths) may be introduced. New URI "roots" may
be introduced too (e.g. `api/catalog/v2`). 

Note that those "v1", "v2", etc. URI path segments are not meant to be 1:1 with Polaris
releases or Polaris project version numbers (e.g. a "v2" path segment does not mean that
it is added in Polaris 2.0).

Polaris servers will support deprecated API endpoints / parameters / versions / etc. 
for some transition period to allow clients to migrate.

### Managing Polaris Database

Polaris stores its data in a database, which is sometimes referred to as "Metastore" or
"Persistence" in other docs.

Each Polaris release may support multiple Persistence [implementations](../metastores),
for example, "EclipseLink" (deprecated) and "JDBC" (current).

Each type of Persistence evolves individually. Within each Persistence type, Polaris
attempts to support rolling upgrades (both version X and X + 1 servers running at the
same time).

However, migrating between different Persistence types is not supported in a rolling
upgrade manner (for example, migrating from "EclipseLink" to "JDBC"). Polaris provides
[tools](https://github.com/apache/polaris-tools/) for migrating between different
catalogs and those tools may be used to migrate between different Persistence types
as well. Service interruption (downtime) should be expected in those cases.

## Using Polaris as a Build-Time Dependency

Polaris produces several jars. These jars or custom builds of Polaris code may be used in
downstream projects according to the terms of the license included into Polaris distributions.

This means that major version increments should be expected often (in the SemVer sense, see
below).

The minimal version of the JRE required by Polaris code (compilation target) may be updated in
any release. Different Polaris jars may have different minimal JRE version requirements.

This approach is not meant to discourage the use of Polaris code in downstream projects, but
to allow more flexibility in evolving the codebase to support new catalog-level features
and improve code efficiency. Maintainers of downstream projects are encouraged to join Polaris 
mailing lists to monitor project changes, suggest improvements, and engage with the Polaris
community in case of specific compatibility concerns.   

## Semantic Versioning

Polaris strives to follow [Semantic Versioning](https://semver.org/) conventions both with
respect to Java code and REST APIs.

The API surface for the purpose of Semantic Versioning is defined as follows.

**Client-Facing API**

* Iceberg REST Catalog API and Generic Tables API (refer to this [link](../polaris-catalog-service/)
for their combined Open API definition).
  * Note: Polaris implementing an optional Iceberg REST Catalog feature that was unimplemented
  in the previous release is not considered a major change.
  * Supporting a new revision of the Iceberg REST Catalog spec in a backward-compatible way
  is not considered a major change.
  * Changing the implementation of an Iceberg REST Catalog feature / endpoint in a non-backward
  compatible way is a major change.
* [Polaris Management API](../polaris-management-service/)
* [Polaris Policies](http://localhost:1313/in-dev/unreleased/policy/)
* Configuration
  * Public (not internal) properties inside Polaris [Entities](../entities/) that affect Polaris behaviour.
  * Runtime [configuration](../configuration/) properties with the `polaris.` name prefix.

**Build-Time Dependencies**

* The `polaris-core` module (`public` classes and interfaces)

**Non-API Modules**

Build-time dependencies other than the `polaris-core` module are considered non-API 
and are not subject to tracking in semantic version number changes. Their `public`
Java interfaces may change without notice even in minor releases.

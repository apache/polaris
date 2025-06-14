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

## Polaris as a Catalog

Polaris is primarily intended to be used as a Catalog of Tables and Views. As such, 
it implements the Iceberg REST API, Polaris Management API and the Generic Tables API.

Revisions of the Iceberg REST API are controlled by the [Apache Iceberg](https://iceberg.apache.org/)
community. Polaris attempts to accurately implement this specification. However, there
is no guarantee that Polaris releases always implement the latest version of the Iceberg
REST Catalog API.

The Polaris Management API is maintained as a versioned REST API. New releases of Polaris may
include changes to the current version of the Management API. When that happens those changes
are intended to be compatible with prior versions of Polaris clients.

In case a major change is required to the Management API that cannot be implemented in a
backward-compatible way, a new version of the API will be introduced. Polaris servers will support
old API version for some transition period to allow clients to migrate. Note that API versions
are not meant to be 1:1 with Polaris releases.

The Generic Tables API is an experimental API at this time. Refer to its documentation page
for more details about compatibility expectations.

## Polaris as a Library

Polaris produces several jars. These jars or custom builds of Polaris code may be used in
downstream projects according to the terms of the license included into Polaris distributions.

However, Polaris' focus as a project is not on ensuring ideal binary compatibility at the java 
class level between releases. Maintainers try to keep binary compatibility on the "best effort"
basis. The main goal being the implementation of Polaris capabilities for access via REST APIs.

This approach is not meant to discourage the use of Polaris code in downstream projects, but
to allow more flexibility in evolving the codebase to support new Catalog-level features
and improve code efficiency. Maintainers of downstream projects are encouraged to join Polaris 
mailing lists to monitor project changes, suggest improvements, and engage with the Polaris
community in case of specific compatibility concerns.   

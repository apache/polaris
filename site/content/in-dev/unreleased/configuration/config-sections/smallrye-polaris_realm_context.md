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
title: smallrye-polaris_realm_context
build:
  list: never
  render: never
---

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.realm-context.realms` |  | `list of string` | The set of realms that are supported by the realm context resolver. The first realm is  considered the default realm.  |
| `polaris.realm-context.header-name` |  | `string` | The header name that contains the realm identifier.  |
| `polaris.realm-context.require-header` |  | `boolean` | Whether to require the realm header to be present in the request. If this is true and the realm  header is not present, the request will be rejected. If this is false and the realm header is  not present, the default realm will be used.   <br><br>Note: this is actually only enforced in production setups. |
| `polaris.realm-context.type` |  | `string` | The type of the realm context resolver. Must be a registered (`org.apache.polaris.service.context.RealmContextResolver`) identifier.  |

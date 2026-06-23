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
title: smallrye-polaris_event_filter
build:
  list: never
  render: never
---

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.event-filter.`_`<name>`_`.type` |  | `string` | The event filter implementation type. Currently supported: `jakarta-el`. |
| `polaris.event-filter.`_`<name>`_`.include` |  | `string` | For Jakarta EL filters only. The expression that events must match. The expression has access  to `type`, `metadata`, and `attributes`. Use `==` for equality  predicates. Only built-in attributes can be specified for now.  |
| `polaris.event-filter.`_`<name>`_`.exclude` |  | `string` | For Jakarta EL filters only. The expression that events must not match. The expression has  access to `type`, `metadata`, and `attributes`. Use `==` for equality  predicates. Only built-in attributes can be specified for now.  |

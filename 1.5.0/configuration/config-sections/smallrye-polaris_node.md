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
title: smallrye-polaris_node
build:
  list: never
  render: never
---

Node management configuration.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.node.lease-duration` | `PT1H` | `duration` | Duration of a node-lease.  |
| `polaris.node.renewal-period` | `PT15M` | `duration` | Time window before the end of a node lease when the lease will be renewed.  |
| `polaris.node.num-nodes` | `1024` | `int` | Maximum number of concurrently active Polaris nodes. Do not change this value or the ID  generator spec, it is a rather internal property. See ID generator spec below.  |
| `polaris.node.id-generator-spec.type` | `snowflake` | `string` |  |
| `polaris.node.id-generator-spec.params.`_`<name>`_ |  | `string` |  |

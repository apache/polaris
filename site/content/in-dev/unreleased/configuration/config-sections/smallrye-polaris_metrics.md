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
title: smallrye-polaris_metrics
build:
  list: never
  render: never
---

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.metrics.tags.`_`<name>`_ |  | `string` | Additional tags to include in the metrics.  |
| `polaris.metrics.realm-id-tag.enable-in-api-metrics` | `false` | `boolean` | Whether to include the Realm ID tag in the API request metrics. <br><br>Beware that if the cardinality of this tag is too high, it can cause performance issues or  even crash the server.  |
| `polaris.metrics.realm-id-tag.enable-in-http-metrics` | `false` | `boolean` | Whether to include the Realm ID tag in the HTTP server request metrics. <br><br>Beware that if the cardinality of this tag is too high, it can cause performance issues or  even crash the server.  |
| `polaris.metrics.realm-id-tag.http-metrics-max-cardinality` | `100` | `int` | The maximum number of Realm ID tag values allowed for the HTTP server request metrics. <br><br>This is used to prevent the number of tags from growing indefinitely and causing  performance issues or crashing the server.   <br><br>If the number of tags exceeds this value, a warning will be logged and no more HTTP server  request metrics will be recorded.  |
| `polaris.metrics.user-principal-tag.enable-in-api-metrics` | `false` | `boolean` | Whether to include the User Principal tag in the API request metrics. <br><br>Beware that if the cardinality of this tag is too high, it can cause performance issues or  even crash the server.  |

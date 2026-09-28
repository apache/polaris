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
title: storage-gcs
build:
  list: never
  render: never
---

| Property | Type | Kind | Description |
|----------|------|------|-------------|
| `gcs.oauth2.token` | `String` | credential | Downscoped OAuth2 access token |
| `gcs.oauth2.token-expires-at` | `String` | credential | Expiration time of the access token, in milliseconds since the Unix epoch |
| `gcs.oauth2.refresh-credentials-endpoint` | `String` | config | Catalog endpoint the client can call to refresh vended credentials before they expire |


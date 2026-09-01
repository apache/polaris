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
title: storage-r2
build:
  list: never
  render: never
---

| Property | Type | Kind | Description |
|----------|------|------|-------------|
| `s3.access-key-id` | `String` | credential | Access key id of the vended R2 temporary credential (the parent token's key id) |
| `s3.secret-access-key` | `String` | credential | Secret access key of the vended R2 temporary credential (derived from the signed JWT) |
| `s3.session-token` | `String` | credential | Session token of the vended R2 temporary credential (base64 of `jwt/` + signed JWT) |
| `s3.session-token-expires-at-ms` | `String` | credential | Expiration time of the R2 temporary credential, in milliseconds since the Unix epoch |
| `s3.endpoint` | `String` | config | R2 S3 endpoint derived from the account id and jurisdiction |
| `s3.path-style-access` | `Boolean` | config | Always true for R2 |
| `client.region` | `String` | config | Always `auto` for R2 |
| `client.refresh-credentials-endpoint` | `String` | config | Catalog endpoint the client can call to refresh vended credentials before they expire |


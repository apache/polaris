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
title: storage-aws
build:
  list: never
  render: never
---

| Property | Type | Kind | Description |
|----------|------|------|-------------|
| `s3.access-key-id` | `String` | credential | AWS access key ID from the STS session |
| `s3.secret-access-key` | `String` | credential | AWS secret access key from the STS session |
| `s3.session-token` | `String` | credential | AWS session token from the STS session |
| `s3.session-token-expires-at-ms` | `String` | credential | Expiration time of the session token, in milliseconds since the Unix epoch |
| `s3.endpoint` | `String` | config | Custom S3 endpoint URL; emitted for S3-compatible stores such as MinIO or Ozone, absent for native AWS S3 |
| `s3.path-style-access` | `Boolean` | config | Set to true when path-style addressing is required; emitted for S3-compatible stores, absent for native AWS S3 |
| `client.region` | `String` | config | AWS region to use for S3 and STS requests |
| `client.refresh-credentials-endpoint` | `String` | config | Catalog endpoint the client can call to refresh vended credentials before they expire |


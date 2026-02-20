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
title: smallrye-polaris_storage
build:
  list: never
  render: never
---

Configuration interface containing parameters for clients accessing S3 services from Polaris  servers.  

Currently, this configuration does not apply to all of Polaris code, but only to select  services.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.storage.aws.access-key` |  | `string` | The AWS access key to use for authentication. If not present, the default credentials provider  chain will be used.  |
| `polaris.storage.aws.secret-key` |  | `string` | The AWS secret key to use for authentication. If not present, the default credentials provider  chain will be used.  |
| `polaris.storage.gcp.token` |  | `string` | The GCP access token to use for authentication. If not present, the default credentials  provider chain will be used.  |
| `polaris.storage.gcp.lifespan` |  | `duration` | The lifespan of the GCP access token. If not present, the default token lifespan (null) will be used.  |
| `polaris.storage.clients-cache-max-size` |  | `int` | Maximum number of entries to keep in the STS clients cache.  |
| `polaris.storage.max-http-connections` |  | `int` | Override the default maximum number of pooled connections.  |
| `polaris.storage.read-timeout` |  | `duration` | Override the default connection read timeout.  |
| `polaris.storage.connect-timeout` |  | `duration` | Override the default TCP connect timeout.  |
| `polaris.storage.connection-acquisition-timeout` |  | `duration` | Override default connection acquisition timeout. This is the time a request will wait for a  connection from the pool.  |
| `polaris.storage.connection-max-idle-time` |  | `duration` | Override default max idle time of a pooled connection.  |
| `polaris.storage.connection-time-to-live` |  | `duration` | Override default time-time of a pooled connection.  |
| `polaris.storage.expect-continue-enabled` |  | `boolean` | Override default behavior whether to expect an HTTP/100-Continue.  |

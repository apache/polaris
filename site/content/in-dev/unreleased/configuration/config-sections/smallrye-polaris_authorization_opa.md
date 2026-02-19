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
title: smallrye-polaris_authorization_opa
build:
  list: never
  render: never
---

Configuration for OPA (Open Policy Agent) authorization. 

Beta Feature: OPA authorization is currently in Beta and is not a stable  release. It may undergo breaking changes in future versions. Use with caution in production  environments.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.authorization.opa.policy-uri` |  | `uri` |  |
| `polaris.authorization.opa.auth.type` | `none` | `NONE, BEARER` | Type of authentication  |
| `polaris.authorization.opa.auth.bearer.static-token.value` |  | `string` | Static bearer token value  |
| `polaris.authorization.opa.auth.bearer.file-based.path` |  | `path` | Path to file containing bearer token  |
| `polaris.authorization.opa.auth.bearer.file-based.refresh-interval` |  | `duration` | How often to refresh file-based bearer tokens (defaults to 5 minutes if not specified)  |
| `polaris.authorization.opa.auth.bearer.file-based.jwt-expiration-refresh` |  | `boolean` | Whether to automatically detect JWT tokens and use their 'exp' field for refresh timing. If  true and the token is a valid JWT with an 'exp' claim, the token will be refreshed based on  the expiration time minus the buffer, rather than the fixed refresh interval. Defaults to  true if not specified.  |
| `polaris.authorization.opa.auth.bearer.file-based.jwt-expiration-buffer` |  | `duration` | Buffer time before JWT expiration to refresh the token. Only used when jwtExpirationRefresh  is true and the token is a valid JWT. Defaults to 1 minute if not specified.  |
| `polaris.authorization.opa.http.timeout` | `PT2S` | `duration` |  |
| `polaris.authorization.opa.http.verify-ssl` | `true` | `boolean` |  |
| `polaris.authorization.opa.http.trust-store-path` |  | `path` |  |
| `polaris.authorization.opa.http.trust-store-password` |  | `string` |  |

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
title: smallrye-polaris_authentication
build:
  list: never
  render: never
---

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.authentication.type` | `internal` | `enum (INTERNAL, EXTERNAL, MIXED)` | The type of authentication to use.  |
| `polaris.authentication.principal-mode` | `internal` | `enum (INTERNAL, EXTERNAL)` | The principal mode to use. Valid values are `INTERNAL` and `EXTERNAL`.   <br><br>Internal mode is the default. When internal mode is used, callers must have a corresponding  principal in the Polaris metastore, which serves as the authoritative source of truth for  principals, roles, and grants.   <br><br>When external mode is used, callers are authenticated entirely from the presented  credentials and are not required to have a corresponding entity in the Polaris metastore. Note  that when the presented credentials were issued by Polaris itself, a backing principal entity  is always required, regardless of the principal mode.   <br><br>External principal mode is only meaningful when an external IDP is used, that is, when the  authentication type (#type()) is (`AuthenticationType#EXTERNAL`) or (`AuthenticationType#MIXED`). Also, this mode is not compatible with the default, built-in Polaris  authorizer; an external authorizer (e.g., OPA or Ranger) must be configured.  |
| `polaris.authentication.authenticator.type` | `default` | `string` | The type of the identity provider. Must be a registered (`Authenticator`) identifier.  |
| `polaris.authentication.token-service.type` | `default` | `string` | The type of the OAuth2 service. Must be a registered (`IcebergRestOAuth2ApiService`) identifier.  |
| `polaris.authentication.token-broker.max-token-generation` | `PT1H` | `duration` | The maximum token duration.  |
| `polaris.authentication.token-broker.type` | `rsa-key-pair` | `string` | The type of the token broker factory. Must be a registered (`TokenBrokerFactory`) identifier.  |
| `polaris.authentication.token-broker.rsa-key-pair.public-key-file` |  | `path` | The path to the public key file.  |
| `polaris.authentication.token-broker.rsa-key-pair.private-key-file` |  | `path` | The path to the private key file.  |
| `polaris.authentication.token-broker.symmetric-key.secret` |  | `string` | The secret to use for both signing and verifying signatures. Either this option of (`#file()`) must be provided.  |
| `polaris.authentication.token-broker.symmetric-key.file` |  | `path` | The file to read the secret from. Either this option of (`#secret()`) must be provided.  |
| `polaris.authentication.`_`<realm>`_`.type` | `internal` | `enum (INTERNAL, EXTERNAL, MIXED)` | The type of authentication to use.  |
| `polaris.authentication.`_`<realm>`_`.principal-mode` | `internal` | `enum (INTERNAL, EXTERNAL)` | The principal mode to use. Valid values are `INTERNAL` and `EXTERNAL`.   <br><br>Internal mode is the default. When internal mode is used, callers must have a corresponding  principal in the Polaris metastore, which serves as the authoritative source of truth for  principals, roles, and grants.   <br><br>When external mode is used, callers are authenticated entirely from the presented  credentials and are not required to have a corresponding entity in the Polaris metastore. Note  that when the presented credentials were issued by Polaris itself, a backing principal entity  is always required, regardless of the principal mode.   <br><br>External principal mode is only meaningful when an external IDP is used, that is, when the  authentication type (#type()) is (`AuthenticationType#EXTERNAL`) or (`AuthenticationType#MIXED`). Also, this mode is not compatible with the default, built-in Polaris  authorizer; an external authorizer (e.g., OPA or Ranger) must be configured.  |
| `polaris.authentication.`_`<realm>`_`.authenticator.type` | `default` | `string` | The type of the identity provider. Must be a registered (`Authenticator`) identifier.  |
| `polaris.authentication.`_`<realm>`_`.token-service.type` | `default` | `string` | The type of the OAuth2 service. Must be a registered (`IcebergRestOAuth2ApiService`) identifier.  |
| `polaris.authentication.`_`<realm>`_`.token-broker.max-token-generation` | `PT1H` | `duration` | The maximum token duration.  |
| `polaris.authentication.`_`<realm>`_`.token-broker.type` | `rsa-key-pair` | `string` | The type of the token broker factory. Must be a registered (`TokenBrokerFactory`) identifier.  |
| `polaris.authentication.`_`<realm>`_`.token-broker.rsa-key-pair.public-key-file` |  | `path` | The path to the public key file.  |
| `polaris.authentication.`_`<realm>`_`.token-broker.rsa-key-pair.private-key-file` |  | `path` | The path to the private key file.  |
| `polaris.authentication.`_`<realm>`_`.token-broker.symmetric-key.secret` |  | `string` | The secret to use for both signing and verifying signatures. Either this option of (`#file()`) must be provided.  |
| `polaris.authentication.`_`<realm>`_`.token-broker.symmetric-key.file` |  | `path` | The file to read the secret from. Either this option of (`#secret()`) must be provided.  |

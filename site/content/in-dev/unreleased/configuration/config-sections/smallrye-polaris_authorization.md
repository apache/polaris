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
title: smallrye-polaris_authorization
build:
  list: never
  render: never
---

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.authorization.type` |  | `string` | <br><br>_Deprecated_ Use `polaris.authorization.authorizer.type` instead. |
| `polaris.authorization.authorizer.type` |  | `string` | The type of the authorizer. Must be a registered (`org.apache.polaris.core.auth.PolarisAuthorizerFactory`) identifier. Defaults to `internal`. <br><br>For the default realm, falls back to the legacy `polaris.authorization.type` property if this property is not set. A realm that sets a `tenant-key` must also set its `type` explicitly. |
| `polaris.authorization.authorizer.tenant-key` |  | `string` | The authorizer tenant key identifying which authorizer configuration to use. When present, refers to a named entry in the authorizer-specific tenant configuration (e.g. `polaris.authorization.opa.<tenant-key>.*`). When absent, the default (unnamed) configuration is used. |
| `polaris.authorization.authorizer.`_`<realm>`_`.type` |  | `string` | The type of the authorizer. Must be a registered (`org.apache.polaris.core.auth.PolarisAuthorizerFactory`) identifier. Defaults to `internal`. <br><br>For the default realm, falls back to the legacy `polaris.authorization.type` property if this property is not set. A realm that sets a `tenant-key` must also set its `type` explicitly. |
| `polaris.authorization.authorizer.`_`<realm>`_`.tenant-key` |  | `string` | The authorizer tenant key identifying which authorizer configuration to use. When present, refers to a named entry in the authorizer-specific tenant configuration (e.g. `polaris.authorization.opa.<tenant-key>.*`). When absent, the default (unnamed) configuration is used. |

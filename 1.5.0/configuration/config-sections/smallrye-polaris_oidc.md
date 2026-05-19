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
title: smallrye-polaris_oidc
build:
  list: never
  render: never
---

Polaris-specific configuration for OIDC tenants.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.oidc.principal-mapper.id-claim-path` |  | `string` | The path to the claim that contains the principal ID. Nested paths can be expressed using "/"  as a separator, e.g. `"resource_access/client1/roles"` would look for the "roles" field  inside the "client1" object inside the "resource_access" object in the token claims.   <br><br>Optional. Either this option or (`#nameClaimPath()`) must be provided.  |
| `polaris.oidc.principal-mapper.name-claim-path` |  | `string` | The claim that contains the principal name. Nested paths can be expressed using "/" as a  separator, e.g. `"resource_access/client1/roles"` would look for the "roles" field  inside the "client1" object inside the "resource_access" object in the token claims.   <br><br>Optional. Either this option or (`#idClaimPath()`) must be provided.  |
| `polaris.oidc.principal-mapper.type` | `default` | `string` | The type of the principal mapper. Must be a registered (`org.apache.polaris.service.auth.external.mapping.PrincipalMapper`) identifier.  |
| `polaris.oidc.principal-roles-mapper.type` | `default` | `string` | The type of the principal roles mapper. Must be a registered (`org.apache.polaris.service.auth.external.mapping.PrincipalRolesMapper`) identifier.  |
| `polaris.oidc.principal-roles-mapper.filter` |  | `string` | A regular expression that matches the role names in the identity. Only roles that match this  regex will be included in the Polaris-specific roles.  |
| `polaris.oidc.principal-roles-mapper.mappings` |  | `list of ` | A list of regex mappings that will be applied to each role name in the identity.  |
| `polaris.oidc.principal-roles-mapper.mappings.regex` |  | `string` | A regular expression that will be applied to each role name in the identity. Along with  (`#replacement()`), this regex is used to transform the role names in the identity into  Polaris-specific roles.  |
| `polaris.oidc.principal-roles-mapper.mappings.replacement` |  | `string` | The replacement string for the role names in the identity. This is used along with (`#regex()`) to transform the role names in the identity into Polaris-specific roles.  |
| `polaris.oidc.`_`<tenant>`_`.principal-mapper.id-claim-path` |  | `string` | The path to the claim that contains the principal ID. Nested paths can be expressed using "/"  as a separator, e.g. `"resource_access/client1/roles"` would look for the "roles" field  inside the "client1" object inside the "resource_access" object in the token claims.   <br><br>Optional. Either this option or (`#nameClaimPath()`) must be provided.  |
| `polaris.oidc.`_`<tenant>`_`.principal-mapper.name-claim-path` |  | `string` | The claim that contains the principal name. Nested paths can be expressed using "/" as a  separator, e.g. `"resource_access/client1/roles"` would look for the "roles" field  inside the "client1" object inside the "resource_access" object in the token claims.   <br><br>Optional. Either this option or (`#idClaimPath()`) must be provided.  |
| `polaris.oidc.`_`<tenant>`_`.principal-mapper.type` | `default` | `string` | The type of the principal mapper. Must be a registered (`org.apache.polaris.service.auth.external.mapping.PrincipalMapper`) identifier.  |
| `polaris.oidc.`_`<tenant>`_`.principal-roles-mapper.type` | `default` | `string` | The type of the principal roles mapper. Must be a registered (`org.apache.polaris.service.auth.external.mapping.PrincipalRolesMapper`) identifier.  |
| `polaris.oidc.`_`<tenant>`_`.principal-roles-mapper.filter` |  | `string` | A regular expression that matches the role names in the identity. Only roles that match this  regex will be included in the Polaris-specific roles.  |
| `polaris.oidc.`_`<tenant>`_`.principal-roles-mapper.mappings` |  | `list of ` | A list of regex mappings that will be applied to each role name in the identity.  |
| `polaris.oidc.`_`<tenant>`_`.principal-roles-mapper.mappings.regex` |  | `string` | A regular expression that will be applied to each role name in the identity. Along with  (`#replacement()`), this regex is used to transform the role names in the identity into  Polaris-specific roles.  |
| `polaris.oidc.`_`<tenant>`_`.principal-roles-mapper.mappings.replacement` |  | `string` | The replacement string for the role names in the identity. This is used along with (`#regex()`) to transform the role names in the identity into Polaris-specific roles.  |
| `polaris.oidc.tenant-resolver.type` | `default` | `string` | The type of the OIDC tenant resolver. Must be a registered (`OidcTenantResolver`) implementation.  |

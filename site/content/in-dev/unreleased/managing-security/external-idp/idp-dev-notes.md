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
title: Authentification Development Details
linkTitle: Development Details
type: docs
weight: 301
---

## Developer Architecture Notes

### Authentication Architecture

Polaris separates authentication into two logical phases using [Quarkus Security](https://quarkus.io/guides/security-overview):

1. Credential extraction – parsing headers and tokens
2. Credential authentication – validating identity and assigning roles

### Key Interfaces

- [`Authenticator`](https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/auth/Authenticator.java): A core interface used to authenticate credentials and resolve principal and principal roles. Roles may be derived from OIDC claims or internal mappings.
- [`InternalPolarisToken`](https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/auth/InternalPolarisToken.java): Used in internal auth and inherits from `PrincipalCredential`.

- The [`DefaultAuthenticator`](https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/auth/DefaultAuthenticator.java) is used to implement realm-specific logic based on these abstractions.

### Token Broker Configuration

When internal authentication is enabled, Polaris uses [`TokenBroker`](https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/auth/TokenBroker.java) to handle the decoding and validation of authentication tokens. These brokers are request-scoped and can be configured per realm. Each realm may use its own strategy, such as RSA key pairs or shared secrets, depending on security requirements.
See [Token Broker description]({{< relref "external-idp#token-broker" >}}) for configuration details.

## Developer Authentication Workflows

### Internal Authentication

1. [`InternalAuthenticationMechanism`](https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/auth/internal/InternalAuthenticationMechanism.java) parses the auth header.
2. Uses [`TokenBroker`](https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/auth/TokenBroker.java) to decode the token.
3. Builds [`InternalAuthenticationRequest`](https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/auth/internal/InternalAuthenticationRequest.java) and generates `SecurityIdentity` (Quarkus).
4. `Authenticator.authenticate()` validates the credential, resolves the principal and principal roles, then creates the `PolarisPrincipal`.

### External Authentication

1. `OidcAuthenticationMechanism` (Quarkus) processes the auth header.
2. [`OidcTenantResolvingAugmentor`](https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/auth/external/tenant/OidcTenantResolvingAugmentor.java) selects the OIDC tenant.
3. [`OidcPolarisCredentialAugmentor`](https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/auth/external/OidcPolarisCredentialAugmentor.java) extracts JWT claims.
4. `Authenticator.authenticate()` validates the claims, resolves the principal and principal roles, then creates the `PolarisPrincipal`.

### Mixed Authentication

1. [`InternalAuthenticationMechanism`](https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/auth/internal/InternalAuthenticationMechanism.java) tries decoding.
2. If successful, proceed with internal authentication.
3. Otherwise, fall back to external (OIDC) authentication.

## OIDC Configuration Reference

### Principal Mapping

- Interface: [`PrincipalMapper`](https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/auth/external/mapping/PrincipalMapper.java)

  The `PrincipalMapper` is responsible for extracting the Polaris principal ID and display name from OIDC tokens.

- Implementation selector:

  This property selects the implementation of the `PrincipalMapper` interface. The default implementation extracts fields from specific claim paths.

  ```properties
  polaris.oidc.principal-mapper.type=default
  ```

- Configuration properties for the default implementation:

  ```properties
  polaris.oidc.principal-mapper.id-claim-path=polaris/principal_id 
  polaris.oidc.principal-mapper.name-claim-path=polaris/principal_name
  ```

- It can be overridden per OIDC tenant.

### Roles Mapping

- Interface: [`PrincipalRolesMapper`](https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/auth/external/mapping/PrincipalRolesMapper.java)

  Polaris uses this component to transform role claims from OIDC tokens into Polaris roles.

- Quarkus OIDC configuration:

  This setting instructs Quarkus on where to locate roles within the OIDC token.

  ```properties
  quarkus.oidc.roles.role-claim-path=polaris/roles
  ```

- Implementation selector:

  This property selects the implementation of `PrincipalRolesMapper`. The `default` implementation applies regular expression (regex) transformations to OIDC roles.

  ```properties
  polaris.oidc.principal-roles-mapper.type=default
  ```

- Configuration properties for the default implementation:

  ```properties
  polaris.oidc.principal-roles-mapper.filter=^(?!profile$|email$).* 
  polaris.oidc.principal-roles-mapper.mappings[0].regex=^.*$ 
  polaris.oidc.principal-roles-mapper.mappings[0].replacement=PRINCIPAL_ROLE:$0
  ```

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
title: External Identity Providers 
type: docs
weight: 550
---

Apache Polaris supports authentication via external identity providers (IdPs) using OpenID Connect (OIDC) in addition to the internal authentication system. This feature enables flexible identity federation with enterprise IdPs and allows gradual migration or hybrid authentication strategies across realms in Polaris. 

## Authentication Types 

Polaris supports three authentication modes: 

1. `internal` (Default) 
   - Only Polaris internal authentication is used. 
2. `external` 
   - Authenticates using external OIDC providers (via Quarkus OIDC). 
   - Disables the internal token endpoint (returns HTTP 501). 
3. `mixed` 
   - Tries internal authentication first; if this fails, it falls back to OIDC. 

Authentication can be configured globally or per realm by setting the following properties: 

```properties
# Global default 
polaris.authentication.type=internal 
# Per-realm override 
polaris.authentication.realm1.type=external 
polaris.authentication.realm2.type=mixed 
```

## Key Components

### Authenticator 

The [`Authenticator`](https://github.com/apache/polaris/blob/main/service/common/src/main/java/org/apache/polaris/service/auth/Authenticator.java) is a component responsible for creating a Polaris principal from the credentials provided by the authentication process. It is common to all authentication types. 

The `type` property is used to define the `Authenticator` implementation. It is overridable per realm: 

```properties
polaris.authentication.authenticator.type=default 
polaris.authentication.realm1.authenticator.type=custom 
```

### Active Roles Provider 

The [`ActiveRolesProvider`](https://github.com/apache/polaris/blob/main/service/common/src/main/java/org/apache/polaris/service/auth/ActiveRolesProvider.java) is a component responsible for determining which roles the principal is requesting and should be activated. It is common to all authentication types. 

Only the `type` property is defined; it is used to define the provider implementation. It is overridable per realm:  

```properties
polaris.authentication.active-roles-provider.type=default 
```

## Internal Authentication Configuration 

### Token Broker 

The [`TokenBroker`](https://github.com/apache/polaris/blob/main/service/common/src/main/java/org/apache/polaris/service/auth/TokenBroker.java) signs and verifies tokens to ensure that they can be validated and remain unaltered. 

```properties
polaris.authentication.token-broker.type=rsa-key-pair 
polaris.authentication.token-broker.max-token-generation=PT1H 
```
 
Two types are available: 

- `rsa-key-pair` (recommended for production): Uses an RSA key pair for token signing and validation. 
- `symmetric-key`: Uses a shared secret for both operations; suitable for single-node deployments or testing. 

The property `polaris.authentication.token-broker.max-token-generation` specifies the maximum validity duration of tokens issued by the internal `TokenBroker`. 

- Format: ISO-8601 duration (e.g., `PT1H` for 1 hour, `PT30M` for 30 minutes). 
- Default: `PT1H`. 

### Token Service 

The Token Service and `TokenServiceConfiguration` (Quarkus) is responsible for issuing and validating tokens (e.g., bearer tokens) for authenticated principals when internal authentication is used. It works in coordination with the `Authenticator` and `TokenBroker`. The default implementation is `default`, and this must be configured when using internal authentication.  

```properties
polaris.authentication.token-service.type=default 
```
 
## External Authentication Configuration 

External authentication is configured via Quarkus OIDC and Polaris-specific OIDC extensions. The following settings are used to integrate with an identity provider and extract identity and role information from tokens.  

### OIDC Tenant Configuration 

At least one OIDC tenant must be explicitly enabled. In Polaris, realms and OIDC tenants are distinct concepts. An OIDC tenant represents a specific identity provider configuration (e.g., `quarkus.oidc.idp1`). A [realm]({{% ref "realm" %}}) is a logical partition within Polaris.

- Multiple realms can share a single OIDC tenant. 
- Each realm can be associated with only one OIDC tenant. 

Therefore, multi-realm deployments can share a common identity provider while still enforcing realm-level scoping. To configure the default tenant: 

```properties
quarkus.oidc.tenant-enabled=true 
quarkus.oidc.auth-server-url=https://auth.example.com/realms/polaris 
quarkus.oidc.client-id=polaris 
```

Alternatively, it is possible to use multiple named tenants. Each OIDC-named tenant is then configured with standard Quarkus settings: 

```properties
quarkus.oidc.oidc-tenant1.auth-server-url=http://localhost:8080/realms/polaris 
quarkus.oidc.oidc-tenant1.client-id=client1 
quarkus.oidc.oidc-tenant1.application-type=service 
```

When using multiple OIDC tenants, it's your responsibility to configure tenant resolution appropriately. See the [Quarkus OpenID Connect Multitenany Guide](https://quarkus.io/guides/security-openid-connect-multitenancy#tenant-resolution).  

### Principal Mapping 

While OIDC tenant resolution is entirely delegated to Quarkus, Polaris requires additional configuration to extract the Polaris principal and its roles from the credentials generated and validated by Quarkus. This part of the authentication process is configured with Polaris-specific properties that map JWT claims to Polaris principal fields: 

```properties
polaris.oidc.principal-mapper.type=default 
polaris.oidc.principal-mapper.id-claim-path=polaris/principal_id 
polaris.oidc.principal-mapper.name-claim-path=polaris/principal_name 
```

These properties are overridable per OIDC tenant: 

```properties
polaris.oidc.oidc-tenant1.principal-mapper.id-claim-path=polaris/principal_id 
polaris.oidc.oidc-tenant1.principal-mapper.name-claim-path=polaris/principal_name 
```

> [!IMPORTANT]: The default implementation of PrincipalMapper can only work with JWT tokens. If your IDP issues opaque tokens instead, you will need to provide a custom implementation. 

### Role Mapping 

Similarly, Polaris requires additional configuration to map roles provided by Quarkus to roles defined in Polaris. The process happens in two phases: first, Quarkus maps the JWT claims to security roles, using the `quarkus.oidc.roles.*` properties; then, Polaris-specific properties are used to map the Quarkus-provided security roles to Polaris roles: 

```properties
quarkus.oidc.roles.role-claim-path=polaris/roles 
polaris.oidc.principal-roles-mapper.type=default 
polaris.oidc.principal-roles-mapper.filter=^(?!profile$|email$).* 
polaris.oidc.principal-roles-mapper.mappings[0].regex=^.*$ 
polaris.oidc.principal-roles-mapper.mappings[0].replacement=PRINCIPAL_ROLE:$0 
```

These mappings can be overridden per OIDC tenant and used across different realms that rely on external identity providers. For example: 

```properties
polaris.oidc.oidc-tenant1.principal-roles-mapper.type=custom 
polaris.oidc.oidc-tenant1.principal-roles-mapper.filter=POLARIS_ROLE:.* 
polaris.oidc.oidc-tenant1.principal-roles-mapper.mappings[0].regex=POLARIS_ROLE:(.*) 
polaris.oidc.oidc-tenant1.principal-roles-mapper.mappings[0].replacement=POLARIS_ROLE:$1 
```

The default `ActiveRolesProvider` expects the security identity to expose role names in the following format: `POLARIS_ROLE:<role name>`. You can use the `filter` and `mappings` properties to adjust the role names as they appear in the JWT claims. 

For example, assume that the security identity produced by Quarkus exposes the following roles: `role_service_admin` and `role_catalog_admin`. Polaris expects `POLARIS_ROLE:service_admin` and `POLARIS_ROLE:catalog_admin` respectively. The following configuration can be used to achieve the desired mapping: 

```properties
# Exclude role names that don't start with "role_" 
polaris.oidc.principal-roles-mapper.filter=role_.* 
# Extract the text after "role_" 
polaris.oidc.principal-roles-mapper.mappings[0].regex=role_(.*) 
# Replace the extracted text with "POLARIS_ROLE:" 
polaris.oidc.principal-roles-mapper.mappings[0].replacement=POLARIS_ROLE:$1 
```

See more examples below. 
 
## Developer Architecture Notes 

The following sections describe internal implementation details for developers who want to understand or extend Polaris authentication. 

### Authentication Architecture 

Polaris separates authentication into two logical phases using [Quarkus Security](https://quarkus.io/guides/security-overview): 

1. Credential extraction – parsing headers and tokens 
2. Credential authentication – validating identity and assigning roles 

### Key Interfaces 

- [`Authenticator`](https://github.com/apache/polaris/blob/main/service/common/src/main/java/org/apache/polaris/service/auth/Authenticator.java): A core interface used to authenticate credentials. 
- [`DecodedToken`](https://github.com/apache/polaris/blob/main/service/common/src/main/java/org/apache/polaris/service/auth/DecodedToken.java): Used in internal auth and inherits from `PrincipalCredential`. 
- [`ActiveRolesProvider`](https://github.com/apache/polaris/blob/main/service/common/src/main/java/org/apache/polaris/service/auth/ActiveRolesProvider.java): Resolves the set of roles associated with the authenticated user for the current request. Roles may be derived from OIDC claims or internal mappings. 

The [`DefaultAuthenticator`](https://github.com/apache/polaris/blob/main/service/common/src/main/java/org/apache/polaris/service/auth/DefaultAuthenticator.java) is used to implement realm-specific logic based on these abstractions. 

### Token Broker Configuration 

When internal authentication is enabled, Polaris uses token brokers to handle the decoding and validation of authentication tokens. These brokers are request-scoped and can be configured per realm. Each realm may use its own strategy, such as RSA key pairs or shared secrets, depending on security requirements. 

## Developer Authentication Workflows 

### Internal Authentication 

1. [`InternalAuthenticationMechanism`](https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/quarkus/auth/internal/InternalAuthenticationMechanism.java) parses the auth header. 
2. Uses [`TokenBroker`](https://github.com/apache/polaris/blob/main/service/common/src/main/java/org/apache/polaris/service/auth/TokenBroker.java) to decode the token. 
3. Builds [`PrincipalAuthInfo`](https://github.com/apache/polaris/blob/main/service/common/src/main/java/org/apache/polaris/service/auth/PrincipalAuthInfo.java) and generates `SecurityIdentity` (Quarkus). 
4. `Authenticator.authenticate()` validates the credential. 
5. [`ActiveRolesProvider`](https://github.com/apache/polaris/blob/main/service/common/src/main/java/org/apache/polaris/service/auth/ActiveRolesProvider.java) assigns roles. 

### External Authentication 

1. `OidcAuthenticationMechanism` (Quarkus) processes the auth header. 
2. [`OidcTenantResolvingAugmentor`](https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/quarkus/auth/external/OidcTenantResolvingAugmentor.java) selects the OIDC tenant. 
3. [`PrincipalAuthInfoAugmentor`](https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/quarkus/auth/external/PrincipalAuthInfoAugmentor.java) extracts JWT claims. 
4. `Authenticator.authenticate()` validates the claims. 
5. [`ActiveRolesProvider`](https://github.com/apache/polaris/blob/main/service/common/src/main/java/org/apache/polaris/service/auth/ActiveRolesProvider.java) assigns roles. 

### Mixed Authentication 

1. [`InternalAuthenticationMechanism`](https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/quarkus/auth/internal/InternalAuthenticationMechanism.java) tries decoding. 
2. If successful, proceed with internal authentication. 
3. Otherwise, fall back to external (OIDC) authentication. 

## OIDC Configuration Reference 

### Principal Mapping 

- Interface: [`PrincipalMapper`](https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/quarkus/auth/external/mapping/PrincipalMapper.java)

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

- Interface: [`PrincipalRolesMapper`](https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/quarkus/auth/external/mapping/PrincipalRolesMapper.java)

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

### Example JWT Mappings 

#### Example 1: Custom Claim Paths 

- JWT 

  ```json
  { 
    "polaris": 
    { 
      "roles": ["PRINCIPAL_ROLE:ALL"], 
      "principal_name": "root", 
      "principal_id": 1 
    } 
  } 
  ```

- Configuration 

  ```properties
  quarkus.oidc.roles.role-claim-path=polaris/roles 
  polaris.oidc.principal-mapper.id-claim-path=polaris/principal_id 
  polaris.oidc.principal-mapper.name-claim-path=polaris/principal_name 
  ```

#### Example 2: Generic OIDC Claims 

- JWT 

  ```json
  { 
    "sub": "1", 
    "scope": "service_admin catalog_admin profile email", 
    "preferred_username": "root" 
  } 
  ```

- Configuration 

  ```properties
  quarkus.oidc.roles.role-claim-path=scope 
  polaris.oidc.principal-mapper.id-claim-path=sub 
  polaris.oidc.principal-mapper.name-claim-path=preferred_username 
  polaris.oidc.principal-roles-mapper.filter=^(?!profile$|email$).* 
  polaris.oidc.principal-roles-mapper.mappings[0].regex=^.*$ 
  polaris.oidc.principal-roles-mapper.mappings[0].replacement=PRINCIPAL_ROLE:$0 
  ```

- Result 

  Polaris roles: `PRINCIPAL_ROLE:service_admin` and `PRINCIPAL_ROLE:catalog_admin` 
 
 
 
 

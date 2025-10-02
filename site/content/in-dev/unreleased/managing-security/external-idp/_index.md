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
title: Identity Providers
linkTitle: Identity Providers
type: docs
weight: 300
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

The `Authenticator` is a component responsible for resolving the principal and the principal roles, and for creating a `PolarisPrincipal` from the credentials provided by the authentication process. It is a central component and is invoked for all types of authentication.

The `type` property is used to define the `Authenticator` implementation. It is overridable per realm: 

```properties
polaris.authentication.authenticator.type=default
polaris.authentication.realm1.authenticator.type=custom
```

## Internal Authentication Configuration 

### Token Broker

The `TokenBroker` signs and verifies tokens to ensure that they can be validated and remain unaltered. 

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

### Role Mapping

When using internal authentication, token requests should include a `scope` parameter that specifies the roles to be activated for the principal. The `scope` parameter is a space-separated list of role names.

The default `ActiveRolesProvider` expects role names to be in the following format: `PRINCIPAL_ROLE:<role name>`.

For example, if the principal has the roles `service_admin` and `catalog_admin` and wants both activated, the `scope` parameter should look like this: 

```properties
scope=PRINCIPAL_ROLE:service_admin PRINCIPAL_ROLE:catalog_admin
```

Here is an example of a full request to the Polaris token endpoint using internal authentication: 

```http request
POST /api/catalog/v1/oauth/tokens HTTP/1.1
Host: polaris.example.com:8181
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE%3Aservice_admin%20PRINCIPAL_ROLE%3Acatalog_admin
```

## External Authentication Configuration 

External authentication is configured via Quarkus OIDC and Polaris-specific OIDC extensions. The following settings are used to integrate with an identity provider and extract identity and role information from tokens.  

### OIDC Tenant Configuration 

At least one OIDC tenant must be explicitly enabled. In Polaris, realms and OIDC tenants are distinct concepts. An OIDC tenant represents a specific identity provider configuration (e.g., `quarkus.oidc.idp1`). A [realm]({{% ref "../../realm" %}}) is a logical partition within Polaris.

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

When using multiple OIDC tenants, it's your responsibility to configure tenant resolution appropriately. See the [Quarkus OpenID Connect Multitenancy Guide](https://quarkus.io/guides/security-openid-connect-multitenancy#tenant-resolution).  

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

{{< alert important >}}
The default implementation of PrincipalMapper can only work with JWT tokens. If your IDP issues opaque tokens instead, you will need to provide a custom implementation.
{{< /alert >}}

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
polaris.oidc.oidc-tenant1.principal-roles-mapper.filter=PRINCIPAL_ROLE:.*
polaris.oidc.oidc-tenant1.principal-roles-mapper.mappings[0].regex=PRINCIPAL_ROLE:(.*)
polaris.oidc.oidc-tenant1.principal-roles-mapper.mappings[0].replacement=PRINCIPAL_ROLE:$1
```

The default `Authenticator` expects the security identity to expose role names in the following format: `PRINCIPAL_ROLE:<role name>`. You can use the `filter` and `mappings` properties to adjust the role names as they appear in the JWT claims. 

For example, assume that the security identity produced by Quarkus exposes the following roles: `role_service_admin` and `role_catalog_admin`. Polaris expects `PRINCIPAL_ROLE:service_admin` and `PRINCIPAL_ROLE:catalog_admin` respectively. The following configuration can be used to achieve the desired mapping: 

```properties
# Exclude role names that don't start with "role_" 
polaris.oidc.principal-roles-mapper.filter=role_.*
# Extract the text after "role_" 
polaris.oidc.principal-roles-mapper.mappings[0].regex=role_(.*)
# Replace the extracted text with "PRINCIPAL_ROLE:" 
polaris.oidc.principal-roles-mapper.mappings[0].replacement=PRINCIPAL_ROLE:$1
```

See more examples below. 

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

### Additional Links 

* For complete Keycloak integration example, see: [Keycloak External IDP Configuration Guide]({{< relref "keycloak-idp.md" >}})
* See [Developer Notes]({{< relref "idp-dev-notes.md" >}}) with internal implementation details for developers who want to understand or extend Polaris authentication.
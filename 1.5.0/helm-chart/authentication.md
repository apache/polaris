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

title: Authentication
linkTitle: Authentication
weight: 300
---

This page describes how to configure authentication for Polaris when deploying with the Helm chart.

## Overview

Polaris supports three authentication modes:

| Mode     | Type       | Description                                                                         |
|----------|------------|-------------------------------------------------------------------------------------|
| Internal | `internal` | Polaris manages credentials and issues tokens. Default mode.                        |
| External | `external` | An external Identity Provider (IDP) issues tokens. Polaris validates them via OIDC. |
| Mixed    | `mixed`    | Both internal and external authentication are enabled.                              |

For more information on authentication in Polaris, see the [Identity Providers]({{% relref "../managing-security/external-idp/" %}}) section of the documentation.

## Internal Authentication

Internal authentication is the default mode. Polaris manages user credentials and issues JWT tokens using either an RSA key pair or a symmetric key.

{{< alert warning >}}
In a multi-replica production environment, all Polaris pods must share the same token signing keys. The default chart generates random keys for each pod, which will cause token validation failures.
{{< /alert >}}

### RSA Key Pair (Recommended)

RSA key pairs provide asymmetric encryption, allowing public key distribution for token verification.

Generate an RSA key pair and create a Kubernetes secret:

```bash
openssl genrsa -out private.pem 2048
openssl rsa -in private.pem -pubout -out public.pem

kubectl create secret generic polaris-token-keys \
  --namespace polaris \
  --from-file=private.pem \
  --from-file=public.pem
```

Configure the chart to use the RSA key pair:

```yaml
authentication:
  type: internal
  tokenBroker:
    type: rsa-key-pair
    secret:
      name: "polaris-token-keys"
      rsaKeyPair:
        publicKey: "public.pem"
        privateKey: "private.pem"
```

### Symmetric Key

Symmetric keys use the same secret for both signing and verification.

Generate a symmetric key and create a Kubernetes secret:

```bash
openssl rand -base64 32 > symmetric.key

kubectl create secret generic polaris-token-keys \
  --namespace polaris \
  --from-file=symmetric.key
```

Configure the chart:

```yaml
authentication:
  type: internal
  tokenBroker:
    type: symmetric-key
    secret:
      name: "polaris-token-keys"
      symmetricKey:
        secretKey: "symmetric.key"
```

### Token Lifetime

Configure the maximum token lifetime:

```yaml
authentication:
  tokenBroker:
    maxTokenGeneration: PT1H  # 1 hour (ISO 8601 duration)
```

## External Authentication (OIDC)

External authentication delegates token issuance to an external Identity Provider (IDP) that supports OpenID Connect (OIDC). Polaris validates tokens using the IDP's public keys.

Polaris works with any OIDC-compliant identity provider.

### Basic Configuration

To enable external authentication:

```yaml
authentication:
  type: external

oidc:
  authServeUrl: "https://your-idp.example.com/realms/polaris"
  client:
    id: polaris
```

### Configuration with Client Secret

If your IDP requires a client secret for token introspection:

```bash
kubectl create secret generic polaris-oidc-client \
  --namespace polaris \
  --from-literal=clientSecret='your-client-secret'
```

```yaml
authentication:
  type: external

oidc:
  authServeUrl: "https://your-idp.example.com/realms/polaris"
  client:
    id: polaris
    secret:
      name: "polaris-oidc-client"
      key: "clientSecret"
```

### Principal Mapping

Configure how Polaris maps OIDC token claims to Polaris principals:

```yaml
oidc:
  principalMapper:
    type: default
    idClaimPath: "sub"                   # Claim containing the principal ID
    nameClaimPath: "preferred_username"  # Claim containing the principal name
```

For nested claims, use `/` as a separator:

```yaml
oidc:
  principalMapper:
    idClaimPath: "polaris/principal_id"
    nameClaimPath: "polaris/principal_name"
```

### Role Mapping

Configure how Polaris maps OIDC token claims to Polaris roles:

```yaml
oidc:
  principalRolesMapper:
    type: default
    rolesClaimPath: "realm_access/roles"  # Path to roles in the token
```

#### Role Filtering

Filter which roles from the token are passed to Polaris:

```yaml
oidc:
  principalRolesMapper:
    filter: "^POLARIS_.*"  # Only include roles starting with POLARIS_
```

#### Role Name Transformation

Transform role names from the IDP format to Polaris format using regex mappings:

```yaml
oidc:
  principalRolesMapper:
    mappings:
      - regex: "^role_(.*)"
        replacement: "PRINCIPAL_ROLE:$1"
      - regex: "^admin$"
        replacement: "PRINCIPAL_ROLE:service_admin"
```

The default Polaris authenticator expects roles in the format `PRINCIPAL_ROLE:<role_name>`.

### Full Example

```yaml
authentication:
  type: external

oidc:
  authServeUrl: "https://keycloak.example.com/realms/polaris"
  client:
    id: polaris
    secret:
      name: "polaris-oidc-client"
      key: "clientSecret"
  principalMapper:
    idClaimPath: "sub"
    nameClaimPath: "preferred_username"
  principalRolesMapper:
    rolesClaimPath: "realm_access/roles"
    mappings:
      - regex: "^polaris_(.*)"
        replacement: "PRINCIPAL_ROLE:$1"
```

## Mixed Authentication

Mixed mode enables both internal and external authentication simultaneously. This is useful during migration from internal to external authentication, or when some clients use internal credentials while others use external tokens.

```yaml
authentication:
  type: mixed
  tokenBroker:
    type: rsa-key-pair
    secret:
      name: "polaris-token-keys"
      rsaKeyPair:
        publicKey: "public.pem"
        privateKey: "private.pem"

oidc:
  authServeUrl: "https://your-idp.example.com/realms/polaris"
  client:
    id: polaris
```

## Per-Realm Authentication

You can configure different authentication settings for different realms:

```yaml
authentication:
  type: internal  # Default for all realms
  realmOverrides:
    production:
      type: external
    staging:
      type: mixed
```

## Advanced Configuration

For advanced OIDC configuration not covered by the chart values, use the `advancedConfig` section to pass Quarkus OIDC properties directly:

```yaml
advancedConfig:
  quarkus.oidc.token.issuer: "https://your-idp.example.com"
  quarkus.oidc.token.audience: "polaris-api"
  quarkus.oidc.authentication.scopes: "openid,profile,email"
```

See the [Quarkus OIDC configuration reference](https://quarkus.io/guides/security-oidc-bearer-token-authentication) for all available options.

## Disabling the Token Service

When using external authentication exclusively, you can disable the internal token service:

```yaml
authentication:
  type: external
  tokenService:
    type: disabled
```

This prevents Polaris from issuing tokens and ensures all authentication goes through the external IDP.


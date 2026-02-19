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
title: Iceberg REST Federation
type: docs
weight: 704
---

Polaris can federate an external Iceberg REST catalog (e.g., another Polaris deployment, AWS Glue, or a custom Iceberg
REST implementation), enabling a Polaris service to access table and view entities managed by remote Iceberg REST Catalogs.

## Runtime requirements

- **REST endpoint:** The remote service must expose the Iceberg REST specification. Configure
  firewalls so Polaris can reach the base URI you provide in the connection config.
- **Authentication:** Polaris forwards requests using the credentials defined in
  `ConnectionConfigInfo.AuthenticationParameters`. OAuth2 client credentials, bearer tokens, and AWS
  SigV4 are supported; choose the scheme the remote service expects.

## Feature configuration

The catalog federation feature is disabled by default. Enable the necessary feature flag in your application.properties
file (or equivalent configuration mechanism such as environment variables or a Kubernetes ConfigMap):

```properties
# Enables the federation feature itself
polaris.features."ENABLE_CATALOG_FEDERATION"=true
```

## Creating a federated REST catalog

The snippet below registers an external catalog that forwards to a remote Polaris server using OAuth2
client credentials. `iceberg-remote-catalog-name` is optional; supply it when the remote server multiplexes
multiple logical catalogs under one URI.

```bash
polaris catalogs create \
    --type EXTERNAL \
    --storage-type s3 \
    --role-arn "arn:aws:iam::123456789012:role/polaris-warehouse-access" \
    --default-base-location "s3://analytics-bucket/warehouse/" \
    --catalog-connection-type iceberg-rest \
    --iceberg-remote-catalog-name analytics \
    --catalog-uri "https://remote-polaris.example.com/catalog/v1" \
    --catalog-authentication-type OAUTH \
    --catalog-token-uri "https://remote-polaris.example.com/catalog/v1/oauth/tokens" \
    --catalog-client-id "<remote-client-id>" \
    --catalog-client-secret "<remote-client-secret>" \
    --catalog-client-scope "PRINCIPAL_ROLE:ALL" \
    analytics_rest
```

Refer to the [CLI documentation](../command-line-interface.md#catalogs) for details on alternative authentication types such as BEARER or SIGV4.

Grant catalog roles to principal roles the same way you do for internal catalogs so compute engines
receive tokens with access to the federated namespace.

## Outbound HTTP settings

Iceberg REST federation uses Iceberg's HTTP client. You can pass through HTTP settings by adding
catalog properties when creating or updating the external catalog (via `--property` or
`--set-property`).

Common settings include:

- `rest.client.proxy.hostname`
- `rest.client.proxy.port`
- `rest.client.proxy.username`
- `rest.client.proxy.password`
- `rest.client.connection-timeout-ms`
- `rest.client.socket-timeout-ms`

Example:

```bash
polaris catalogs update analytics_rest \
    --set-property rest.client.proxy.hostname=proxy.example.com \
    --set-property rest.client.proxy.port=3128 \
    --set-property rest.client.connection-timeout-ms=30000 \
    --set-property rest.client.socket-timeout-ms=120000
```

Connection config properties (URI and authentication) take precedence if the same keys are present.

## Operational notes

- **Connectivity checks:** Polaris does not lazily probe the remote service; catalog creation fails if
  the REST endpoint is unreachable or authentication is rejected.
- **Feature parity:** Federation exposes whatever table/namespace operations the remote service
  implements. Unsupported features return the remote error directly to callers.
- **Generic tables:** The REST federation path currently surfaces Iceberg tables only; generic table
  federation is not implemented.

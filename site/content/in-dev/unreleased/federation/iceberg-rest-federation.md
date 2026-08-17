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
  `ConnectionConfigInfo.AuthenticationParameters`. OAuth2 client credentials, bearer tokens, GCP,
  and AWS SigV4 are supported; choose the scheme the remote service expects.

  SigV4 comes in two forms. Supply `roleArn` for AWS services such as Glue: Polaris uses its own
  AWS IAM service identity to assume that role through AWS STS. Supply `accessKeyId` and
  `secretAccessKey` instead for SigV4-compatible catalogs that are not AWS, which have no role to
  assume and no AWS STS endpoint to reach; the keys are used for signing directly. The two forms
  are mutually exclusive. The secret access key is offloaded to the configured secrets manager and
  is never returned by the API.

## Feature configuration

The catalog federation feature is disabled by default. Enable the necessary feature flag in your application.properties
file (or equivalent configuration mechanism such as environment variables or a Kubernetes ConfigMap):

```properties
# Enables the federation feature itself
polaris.features."ENABLE_CATALOG_FEDERATION"=true
```

## Storage credentials

By default a federated catalog vends storage credentials it mints itself, from the
`storageConfigInfo` on the Polaris catalog. That is the right behaviour when the remote catalog's
tables live in storage this Polaris deployment is also configured for.

It does not work when the remote catalog owns the storage. In that case Polaris has no
configuration describing it, the table locations fall outside this catalog's `allowedLocations`, and
the remote may only accept credentials it issued itself. Turn on passthrough so the credentials the
remote vended are handed to the client unchanged:

```properties
polaris.features."FEDERATED_CATALOG_CREDENTIAL_PASSTHROUGH"=true
```

This is a realm-level setting and cannot be set per catalog, and it additionally requires
`ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING`.

Each credential is forwarded with its prefix intact, so a client sees what it would have seen
talking to the remote catalog directly. Clients must understand the `storage-credentials` field of
the load-table response, which Iceberg clients have supported since 1.7. Where the remote returns
several credentials they are forwarded as a list only; a single credential is additionally
flattened into the response config, which two credentials cannot be without colliding.

Two consequences of the credentials being scoped by the remote rather than by this deployment:

- The local allowed-locations check does not apply and is skipped on this path.
- Polaris cannot narrow the credentials to the requesting principal's grants, so passthrough is
  only available to principals authorized for write delegation. A read-only principal requesting
  vended credentials is refused rather than handed credentials wider than its grants.
- The credentials expire on the remote's schedule. Polaris does not supply a refresh endpoint for
  them; a client that needs to refresh must use whatever the remote advertised in its own config.

Polaris only vends when the client asks for it, so the client has to send
`X-Iceberg-Access-Delegation: vended-credentials`. Worth checking first if reads fail against a
federated catalog that works when addressed directly: a remote that vends unconditionally will have
been hiding the missing header. With the Iceberg client, set it as a catalog property:

```
header.X-Iceberg-Access-Delegation=vended-credentials
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

Refer to the [CLI documentation]({{% ref "../command-line-interface.md#catalogs" %}}) for details on alternative authentication types such as BEARER or SIGV4.

Grant catalog roles to principal roles the same way you do for internal catalogs so compute engines
receive tokens with access to the federated namespace.

## Outbound HTTP settings

Iceberg REST federation uses Iceberg's HTTP client. You can pass through non-sensitive HTTP
settings by adding catalog properties when creating or updating the external catalog (via
`--property` or `--set-property`).

{{< alert warning >}}
Catalog properties are returned to authenticated catalog clients as part of the Iceberg REST
`/config` response. Only place non-sensitive client configuration in catalog properties. Do not put
passwords, tokens, access keys, or other secrets into catalog properties.
{{< /alert >}}

Common settings include:

- `rest.client.proxy.hostname`
- `rest.client.proxy.port`
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
- **Register table overwrite:** `POST /v1/{catalog}/namespaces/{namespace}/register` with
  `overwrite=true` is currently supported only for internal Polaris catalogs.
  For federated external catalogs, Polaris rejects overwrite registration requests.
- **Generic tables:** The REST federation path currently surfaces Iceberg tables only; generic table
  federation is not implemented.

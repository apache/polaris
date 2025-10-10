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

Polaris can front an external Iceberg REST catalog so that existing metadata services (another
Polaris deployment, AWS Glue, or any custom Iceberg REST implementation) gain OAuth-protected access
control and multi-engine routing through the Polaris API surface.

## Runtime requirements

- **REST endpoint:** The remote service must expose the Iceberg REST specification. Configure
  firewalls so Polaris can reach the base URI you provide in the connection config.
- **Authentication:** Polaris forwards requests using the credentials defined in
  `ConnectionConfigInfo.AuthenticationParameters`. OAuth2 client credentials, bearer tokens, and AWS
  SigV4 are supported; choose the scheme the remote service expects.
- **Service identity (SigV4 only):** When using SigV4, set `polaris.service-identity.<realm>.aws-iam.*`
  so Polaris can assume the IAM role referenced by the connection’s `serviceIdentity` block.

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
    --catalog-client-id federation-client \
    --catalog-client-secret "<remote-secret>" \
    --catalog-client-scopes "PRINCIPAL_ROLE:ALL" \
    analytics_rest
```

For bearer-token authentication, replace the `authenticationParameters` block with
`{"authenticationType": "BEARER", "bearerToken": "<token>"}`. For SigV4, supply
`{"authenticationType": "SIGV4", ...}` along with an `awsIam` `serviceIdentity` that contains the
role ARN Polaris should assume when signing requests.

Grant catalog roles to principal roles the same way you do for internal catalogs so compute engines
receive tokens with access to the federated namespace.

## Operational notes

- **Connectivity checks:** Polaris does not lazily probe the remote service; catalog creation fails if
  the REST endpoint is unreachable or authentication is rejected.
- **Feature parity:** Federation exposes whatever table/namespace operations the remote service
  implements. Unsupported features return the remote error directly to callers.
- **Generic tables:** The REST federation path currently surfaces Iceberg tables only; generic table
  federation is not implemented.

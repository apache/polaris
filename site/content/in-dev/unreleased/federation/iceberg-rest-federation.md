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
Polaris deployment, Nessie, or a custom Iceberg REST implementation) gain OAuth-protected access
control and multi-engine routing through the Polaris API surface.

## Runtime requirements

- **REST endpoint:** The remote service must expose the Iceberg REST specification. Configure
  firewalls so Polaris can reach the base URI you provide in the connection config.
- **Authentication:** Polaris forwards requests using the credentials defined in
  `connectionConfigInfo.authenticationParameters`. OAuth2 client credentials, bearer tokens, and AWS
  SigV4 are supported; choose the scheme the remote service expects.
- **Service identity (SigV4 only):** When using SigV4, set `polaris.service-identity.<realm>.aws-iam.*`
  so Polaris can assume the IAM role referenced by the connection’s `serviceIdentity` block.
- **Object storage access:** Polaris still writes Iceberg metadata locally when brokering commits, so
  ensure the catalog’s `storageConfigInfo` grants access to the table locations, just as you would for
  internal catalogs.

## Creating a federated REST catalog

The snippet below registers an external catalog that forwards to a remote Polaris server using OAuth2
client credentials. `remoteCatalogName` is optional; supply it when the remote server multiplexes
multiple logical catalogs under one URI.

```bash
curl -X POST https://<polaris-host>/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "type": "EXTERNAL",
        "name": "analytics_rest",
        "storageConfigInfo": {
          "storageType": "S3",
          "roleArn": "arn:aws:iam::123456789012:role/polaris-warehouse-access"
        },
        "properties": { "default-base-location": "s3://analytics-bucket/warehouse/" },
        "connectionConfigInfo": {
          "connectionType": "ICEBERG_REST",
          "uri": "https://remote-polaris.example.com/catalog/v1",
          "remoteCatalogName": "analytics",
          "authenticationParameters": {
            "authenticationType": "OAUTH",
            "tokenUri": "https://remote-polaris.example.com/catalog/v1/oauth/tokens",
            "clientId": "federation-client",
            "clientSecret": "<remote-secret>",
            "scopes": ["PRINCIPAL_ROLE:ALL"]
          }
        }
      }'
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

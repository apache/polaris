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
linkTitle: "REST API with curl"
title: "Using the Polaris REST APIs with curl"
description: "Step-by-step curl examples for the Management and Iceberg REST APIs: tokens, catalogs, principals, roles, grants, namespaces and tables."
weight: 20
tags:
   - rest
   - curl
cascade:
    type: guides
menus:
    main:
        parent: Guides
        weight: 20
---

This guide shows how to use the Polaris REST APIs with plain `curl`, without the Polaris CLI or
a query engine. It covers the steps most setups need:

1. Get an access token.
2. Create a catalog (Management API).
3. Create a principal and give it access to the catalog (Management API).
4. Create namespaces and tables as that principal (Iceberg REST API).
5. Clean up.

The catalog uses `FILE` storage inside the Polaris container, so no cloud account or object
store is needed. **This setup is not intended for production use.**

## Prerequisites

- Docker and Docker Compose v2.
- [`jq`](https://jqlang.org/download/), to read values from JSON responses.

Run all commands from the root of the Polaris source repository. The larger request bodies are
in [`site/content/guides/assets/rest-api/`](https://github.com/apache/polaris/tree/main/site/content/guides/assets/rest-api)
and are passed to `curl` with `-d @<file>`.

## Start Polaris

Start Polaris with an in-memory metastore. The root credentials are `root` / `s3cr3t`:

```shell
docker compose -f site/content/guides/rest-api/docker-compose.yml up
```

Check that Polaris is up. The health endpoint is on the management port, `8182`:

```shell
curl -s http://localhost:8182/q/health | jq .
```

## Get an access token

Polaris APIs expect an OAuth2 bearer token. Request one for the root principal with the
`client_credentials` grant:

```shell
export ROOT_TOKEN=$(curl -s http://localhost:8181/api/catalog/v1/oauth/tokens \
  --user root:s3cr3t \
  -d grant_type=client_credentials \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
```

`--user` sends the client ID and secret as HTTP Basic authentication.
`scope=PRINCIPAL_ROLE:ALL` activates all principal roles granted to the principal.

Polaris can serve several realms. Requests without a `Polaris-Realm` header go to the default
realm, which is `POLARIS` in this guide. To target another realm, add
`-H "Polaris-Realm: <realm>"` to every request, including the token request.

## Management API: create a catalog

Create a catalog named `quickstart_catalog` using
[`create-catalog.json`](https://github.com/apache/polaris/blob/main/site/content/guides/assets/rest-api/create-catalog.json):

```shell
curl -s -X POST http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $ROOT_TOKEN" \
  -H "Content-Type: application/json" \
  -d @site/content/guides/assets/rest-api/create-catalog.json | jq .
```

List the catalogs:

```shell
curl -s http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $ROOT_TOKEN" | jq .
```

## Management API: create a principal and grant access

Clients and engines should not use the root credentials. Create a principal for them instead.
The response contains the new principal's credentials, which are only returned once:

```shell
PRINCIPAL_RESPONSE=$(curl -s -X POST http://localhost:8181/api/management/v1/principals \
  -H "Authorization: Bearer $ROOT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"principal": {"name": "quickstart_user"}}')
export USER_CLIENT_ID=$(echo "$PRINCIPAL_RESPONSE" | jq -r .credentials.clientId)
export USER_CLIENT_SECRET=$(echo "$PRINCIPAL_RESPONSE" | jq -r .credentials.clientSecret)
```

Privileges are granted to catalog roles, catalog roles are assigned to principal roles, and
principal roles are assigned to principals. See
[Entities](/releases/latest/entities/) and [Access Control](/releases/latest/managing-security/access-control/)
for details.

Create a principal role and a catalog role:

```shell
curl -s -X POST http://localhost:8181/api/management/v1/principal-roles \
  -H "Authorization: Bearer $ROOT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"principalRole": {"name": "quickstart_user_role"}}' | jq .

curl -s -X POST http://localhost:8181/api/management/v1/catalogs/quickstart_catalog/catalog-roles \
  -H "Authorization: Bearer $ROOT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole": {"name": "quickstart_catalog_role"}}' | jq .
```

Assign the principal role to the principal, and the catalog role to the principal role:

```shell
curl -s -X PUT http://localhost:8181/api/management/v1/principals/quickstart_user/principal-roles \
  -H "Authorization: Bearer $ROOT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"principalRole": {"name": "quickstart_user_role"}}'

curl -s -X PUT http://localhost:8181/api/management/v1/principal-roles/quickstart_user_role/catalog-roles/quickstart_catalog \
  -H "Authorization: Bearer $ROOT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole": {"name": "quickstart_catalog_role"}}'
```

Grant `CATALOG_MANAGE_CONTENT` to the catalog role. It allows creating, listing, reading and
writing all namespaces and tables in the catalog:

```shell
curl -s -X PUT http://localhost:8181/api/management/v1/catalogs/quickstart_catalog/catalog-roles/quickstart_catalog_role/grants \
  -H "Authorization: Bearer $ROOT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}'
```

List the grants of the catalog role:

```shell
curl -s http://localhost:8181/api/management/v1/catalogs/quickstart_catalog/catalog-roles/quickstart_catalog_role/grants \
  -H "Authorization: Bearer $ROOT_TOKEN" | jq .
```

## Iceberg REST API: namespaces and tables

From here on, use the new principal. Request a token with its credentials:

```shell
export USER_TOKEN=$(curl -s http://localhost:8181/api/catalog/v1/oauth/tokens \
  --user "$USER_CLIENT_ID:$USER_CLIENT_SECRET" \
  -d grant_type=client_credentials \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
```

Iceberg clients start by reading the catalog configuration. The `warehouse` parameter is the
catalog name, and the returned `prefix` is used in all following paths. Here the prefix is the
catalog name, which is why the paths below start with `quickstart_catalog`:

```shell
curl -s "http://localhost:8181/api/catalog/v1/config?warehouse=quickstart_catalog" \
  -H "Authorization: Bearer $USER_TOKEN" | jq .overrides
```

Create a namespace and list the namespaces:

```shell
curl -s -X POST http://localhost:8181/api/catalog/v1/quickstart_catalog/namespaces \
  -H "Authorization: Bearer $USER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"namespace": ["quickstart_namespace"]}' | jq .

curl -s http://localhost:8181/api/catalog/v1/quickstart_catalog/namespaces \
  -H "Authorization: Bearer $USER_TOKEN" | jq .
```

Create a table using
[`create-table.json`](https://github.com/apache/polaris/blob/main/site/content/guides/assets/rest-api/create-table.json),
which defines an Iceberg schema with two columns:

```shell
curl -s -X POST http://localhost:8181/api/catalog/v1/quickstart_catalog/namespaces/quickstart_namespace/tables \
  -H "Authorization: Bearer $USER_TOKEN" \
  -H "Content-Type: application/json" \
  -d @site/content/guides/assets/rest-api/create-table.json | jq '."metadata-location"'
```

List the tables in the namespace, then load the table to see its metadata:

```shell
curl -s http://localhost:8181/api/catalog/v1/quickstart_catalog/namespaces/quickstart_namespace/tables \
  -H "Authorization: Bearer $USER_TOKEN" | jq .

curl -s http://localhost:8181/api/catalog/v1/quickstart_catalog/namespaces/quickstart_namespace/tables/quickstart_table \
  -H "Authorization: Bearer $USER_TOKEN" | jq .metadata.schemas
```

<!--
```shell
curl -s http://localhost:8181/api/catalog/v1/quickstart_catalog/namespaces/quickstart_namespace/tables \
  -H "Authorization: Bearer $USER_TOKEN" | jq -e '.identifiers[] | select(.name == "quickstart_table")'
```
-->

## Clean up

Drop the table and the namespace:

```shell
curl -s -X DELETE http://localhost:8181/api/catalog/v1/quickstart_catalog/namespaces/quickstart_namespace/tables/quickstart_table \
  -H "Authorization: Bearer $USER_TOKEN"

curl -s -X DELETE http://localhost:8181/api/catalog/v1/quickstart_catalog/namespaces/quickstart_namespace \
  -H "Authorization: Bearer $USER_TOKEN"
```

Dropping a table this way removes it from the catalog but leaves its files in storage.
Adding `?purgeRequested=true` also deletes the files, but Polaris rejects it unless purging is
enabled, either server-wide with the `DROP_WITH_PURGE_ENABLED` feature flag or per catalog with the
`polaris.config.drop-with-purge.enabled` catalog property.

A catalog can only be dropped when it is empty, which includes catalog roles other than
`catalog_admin`. Delete the catalog role first, then the catalog, the principal role and the
principal:

```shell
curl -s -X DELETE http://localhost:8181/api/management/v1/catalogs/quickstart_catalog/catalog-roles/quickstart_catalog_role \
  -H "Authorization: Bearer $ROOT_TOKEN"

curl -s -X DELETE http://localhost:8181/api/management/v1/catalogs/quickstart_catalog \
  -H "Authorization: Bearer $ROOT_TOKEN"

curl -s -X DELETE http://localhost:8181/api/management/v1/principal-roles/quickstart_user_role \
  -H "Authorization: Bearer $ROOT_TOKEN"

curl -s -X DELETE http://localhost:8181/api/management/v1/principals/quickstart_user \
  -H "Authorization: Bearer $ROOT_TOKEN"
```

<!--
```shell
curl -s http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $ROOT_TOKEN" | jq -e '.catalogs == []'

curl -s http://localhost:8181/api/management/v1/principals \
  -H "Authorization: Bearer $ROOT_TOKEN" | jq -e 'all(.principals[]; .name != "quickstart_user")'

curl -s http://localhost:8181/api/management/v1/principal-roles \
  -H "Authorization: Bearer $ROOT_TOKEN" | jq -e 'all(.roles[]; .name != "quickstart_user_role")'
```
-->

Stop Polaris:

```shell
docker compose -f site/content/guides/rest-api/docker-compose.yml down
```

## Next Steps

- The full API definitions are in the [Management API](/releases/latest/polaris-api-specs/polaris-management-api/)
  and [Catalog API](/releases/latest/polaris-api-specs/polaris-catalog-api/) references.
- [Polaris CLI](/releases/latest/command-line-interface/), which wraps the same APIs.
- [Spark Integration](/guides/spark), to read and write the table data.

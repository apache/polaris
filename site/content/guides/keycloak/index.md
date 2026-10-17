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
linkTitle: "Authentication: Keycloak"
title: "Getting Started with Apache Polaris, External Authentication and Keycloak"
description: "Uses Keycloak as an external identity provider (IDP) for Polaris authentication."
weight: 100
tags:
  - authorization
  - keycloak
  - idp
cascade:
    type: guides
menus:
    main:
        parent: Guides
        weight: 100
---

## Overview

This example uses Keycloak as an **external** identity provider for Polaris, while keeping Polaris's **built-in**
(internal) authorizer, which authorizes requests using grants recorded in the Polaris metastore. The "iceberg" realm
is automatically created and configured from the `iceberg-realm.json` file.

This Keycloak realm contains 1 client definition, `client1:s3cr3t`, and 1 real user, `keycloak-admin:s3cr3t`, who is
granted the `service_admin` role on `client1`. Unlike the claims used in some other Polaris examples, nothing here is
hardcoded: the token's claims come naturally from this real user and the role granted to it in Keycloak.

- `preferred_username`: a standard OIDC claim, set to the authenticated user's username, "keycloak-admin".
- `resource_access.client1.roles`: a standard OIDC claim listing the authenticated user's roles for the `client1`
  client, `["service_admin"]`.

Note that there is no `principal_id` claim, and `polaris.oidc.principal-mapper.id-claim-path` is intentionally left
unset: **principals should always be matched by name when using an external IDP**. Polaris principal IDs are an
internal implementation detail, assigned by the metastore and never exposed by the management API, so there is no
reliable way for an IDP to know, ahead of time, what ID a given principal will be assigned.

Because the built-in authorizer requires every principal to exist in the Polaris metastore, this example creates a
real Polaris principal named `keycloak-admin` and grants it the `service_admin` principal role, so that tokens
issued by Keycloak for `keycloak-admin` resolve to an authorized principal. See
[Starting the Example](#starting-the-example) below for how this is set up.

Polaris is configured with authentication type `mixed`, which is required for two reasons:

- It keeps the internal token endpoint active, which is needed once during setup (see below).
- It accepts tokens issued by Keycloak, which is needed to authenticate as `keycloak-admin`.

For more information about how to configure Polaris with external authentication, see the
[IDP integration documentation](/releases/latest/managing-security/external-idp/).

If you are instead interested in **fully external principals** — principals that never need to exist in the Polaris
metastore at all — see the [Keycloak + OPA example](/guides/keycloak-opa/), which uses an external
authorizer ([OPA](https://www.openpolicyagent.org/)) instead of the built-in one. The table below compares both
examples:

| Example                                                                  | Authentication Type | Credential Mode      | Principal Pre-sync Required? | Comments                                                                                                     |
|--------------------------------------------------------------------------|---------------------|----------------------|------------------------------|--------------------------------------------------------------------------------------------------------------|
| Keycloak + Internal Principals + Built-in Authorizer (this guide)        | `mixed`             | `internal` (default) | Yes                          | The principal must already exist in Polaris, matched by name, with the roles it needs granted ahead of time. |
| [Keycloak + External Principals + OPA](/guides/keycloak-opa/) | `external`          | `external`           | No                           | The principal is authenticated and authorized entirely from the token; it never needs to exist in Polaris.   |

## Starting the Example

1. Build the Polaris server image if it's not already present locally:

    ```shell
    ./gradlew \
       :polaris-server:assemble \
       :polaris-server:quarkusAppPartsBuild --rerun \
       -Dquarkus.container-image.build=true
    ```

2. Start the docker compose group by running the following command from the root of the repository:

    ```shell
    docker compose -f site/content/guides/keycloak/docker-compose.yml up
    ```

    This also runs a setup step that creates the `keycloak-admin` principal and grants it the `service_admin`
    principal role. This setup step uses the bootstrap `root` principal's own, internally-issued Polaris token to do
    so; `root` is never used to call the REST API for anything else, and in particular is never used to authenticate
    as a caller in the examples below.

## Requesting a Token

Note: the commands below require `jq` to be installed on your machine.

1. Open a terminal and run the following command to request an access token from Keycloak on behalf of the
   `keycloak-admin` user:

    ```shell
    keycloak_token=$(curl -s http://keycloak:8080/realms/iceberg/protocol/openid-connect/token \
      --resolve keycloak:8080:127.0.0.1 \
      --user client1:s3cr3t \
      -d 'grant_type=password' \
      -d 'username=keycloak-admin' \
      -d 'password=s3cr3t' | jq -r .access_token)
    ```

Note the `--resolve` option: it is used to send the request with the `Host` header set to `keycloak`. This is necessary
because Keycloak issues tokens with the `iss` claim matching the request's `Host` header; without this, the token would
not be valid when used against Polaris because the `iss` claim would be `127.0.0.1`, but Polaris expects it to be
`keycloak`, since that's Keycloak's hostname within the Docker network.

This uses the "password" grant (also known as Resource Owner Password Credentials, or ROPC), which lets a client
obtain a token directly from a username and password, without a browser-based redirect. **This grant is deprecated by
OAuth 2.1 and should not be used in production**; it is used here only because it is the simplest way to obtain a
token tied to a real user identity in a non-interactive, scriptable guide. A production integration would instead use
the `authorization_code` grant, which requires a browser to complete the login redirect.

This token is valid for 1 hour and authenticates as `keycloak-admin`.

You can also access the Keycloak admin console. Open a browser and go to [http://localhost:8080](http://localhost:8080),
then log in with the username `admin` and password `admin` (you can change this in the docker-compose file).

## Accessing Polaris with the Token

Open a terminal and run the following command to list the catalogs:

```shell
curl -v http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $keycloak_token" \
  -H 'Polaris-Realm: POLARIS' \
  -H 'Accept: application/json'
```

This succeeds because `keycloak-admin` is a real Polaris principal, granted the `service_admin` principal role during
setup, and the token's `preferred_username` claim ("keycloak-admin") matches it.

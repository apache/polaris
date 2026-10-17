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
linkTitle: "Authentication: Keycloak + OPA"
title: "Getting Started with Apache Polaris, Fully External Principals, Keycloak and OPA"
description: "Uses Keycloak as an external identity provider and OPA as an external authorizer to support fully external principals."
weight: 100
tags:
  - authorization
  - keycloak
  - opa
  - idp
  - external-principals
cascade:
    type: guides
menus:
    main:
        parent: Guides
        weight: 100
---

## Overview

This example uses Keycloak as an **external** identity provider for Polaris. Unlike the [Keycloak
IDP example](/guides/keycloak/), it demonstrates **fully external principals**: a
principal authenticated by Keycloak that never needs to exist in the Polaris metastore at all.

Enabling fully external principals is done by setting the option
`polaris.authentication.credential-mode=external`; it has two hard requirements, both enforced by
Polaris at startup:

1. The authentication type must not be the default (`internal`) one.
2. The authorizer must not be the default (`internal`) one, since it authorizes requests using metastore-backed
   grants, which external principals don't have.

This example satisfies both requirements: the default (and only) realm delegates authentication to Keycloak
exclusively (`polaris.authentication.type=external`), and the whole deployment is authorized by
[Open Policy Agent (OPA)](https://www.openpolicyagent.org/) instead of Polaris's built-in authorizer.

As in the Keycloak IDP example, the "iceberg" Keycloak realm is automatically created and configured from the
`iceberg-realm.json` file, and contains 1 client definition, `client1:s3cr3t`, and 1 real user, `keycloak-admin:s3cr3t`,
who is granted the `service_admin` role on `client1`. Unlike the claims used in some other Polaris examples, nothing
here is hardcoded: the token's claims come naturally from this real user and the role granted to it in Keycloak.

- `preferred_username`: a standard OIDC claim, set to the authenticated user's username, "keycloak-admin".
- `resource_access.client1.roles`: a standard OIDC claim listing the authenticated user's roles for the `client1`
  client, `["service_admin"]`.

Unlike the original Keycloak IDP example, the `keycloak-admin` principal named in Keycloak tokens is **never created
in Polaris**: it is authenticated and authorized entirely from the token. Since authentication is fully external,
Polaris's own internal token endpoint is disabled for this realm; the only way to obtain a usable token is from
Keycloak.

Note that there is no `principal_id` claim, and `polaris.oidc.principal-mapper.id-claim-path` is intentionally left
unset: **principals should always be matched by name when using an external IDP**. This is especially true for fully
external principals, which have no metastore entity to resolve by ID in the first place, but it also holds for the
built-in-authorizer example above, since Polaris principal IDs are an internal implementation detail that isn't
exposed by the management API.

The Keycloak-issued `keycloak-admin` principal is unrelated to the `root` service account principal configured via
`POLARIS_BOOTSTRAP_CREDENTIALS` in the docker-compose file: `root` only exists to initialize Polaris's metastore for
this realm, and is never used to call the REST API, since both the authentication type and the credential mode are
external — Polaris never even accepts a token issued to "root" here.

For more information about how to configure Polaris with external authentication and external principals, see the
[IDP integration documentation](/releases/latest/managing-security/external-idp/). The table below compares
this example with the [Keycloak IDP example](/guides/keycloak/):

| Example                                                                              | Authentication Type | Credential Mode      | Principal Pre-sync Required? | Comments                                                                                                     |
|--------------------------------------------------------------------------------------|---------------------|----------------------|------------------------------|--------------------------------------------------------------------------------------------------------------|
| [Keycloak + Internal Principals + Built-in Authorizer](/guides/keycloak/) | `mixed`             | `internal` (default) | Yes                          | The principal must already exist in Polaris, matched by name, with the roles it needs granted ahead of time. |
| Keycloak + External Principals + OPA (this guide)                                    | `external`          | `external`           | No                           | The principal is authenticated and authorized entirely from the token; it never needs to exist in Polaris.   |

## Authorization with OPA

Because there are no metastore-backed grants for this external principal, Polaris is configured to authorize the
realm with [OPA](/releases/latest/managing-security/external-pdp/opa/) instead of its built-in authorizer. The
policy used by this example is defined in `polaris-authz.rego`:

```rego
package polaris.authz

import future.keywords.if
import future.keywords.in

# Deny by default: every operation must be explicitly allowed below.
default allow := false

# Principals with the "service_admin" role can manage catalogs, namespaces and tables.
allow if {
	"service_admin" in input.actor.roles
	input.action in {
		"LIST_CATALOGS",
		"CREATE_CATALOG",
		"GET_CATALOG",
		"UPDATE_CATALOG",
		"DELETE_CATALOG",
		"LIST_NAMESPACES",
		"CREATE_NAMESPACE",
		"LOAD_NAMESPACE_METADATA",
		"UPDATE_NAMESPACE_PROPERTIES",
		"DROP_NAMESPACE",
		"LIST_TABLES",
		"CREATE_TABLE_DIRECT",
		"LOAD_TABLE",
		"LOAD_TABLE_WITH_READ_DELEGATION",
		"LOAD_TABLE_WITH_WRITE_DELEGATION",
		"UPDATE_TABLE",
		"DROP_TABLE_WITHOUT_PURGE",
		"DROP_TABLE_WITH_PURGE",
	}
}
```

The roles sent to OPA for a fully external principal are whatever the `resource_access.client1.roles` claim of the
token contains, after going through the [Role Mapping](/releases/latest/managing-security/external-idp/#role-mapping)
configuration. This example does not customize the role mapper, so the claim's values (e.g. `service_admin`) are
passed through to OPA unprefixed and unchanged.

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
    docker compose -f site/content/guides/keycloak-opa/docker-compose.yml up
    ```

## Requesting a Token

Note: the commands below require `jq` to be installed on your machine.

Since this realm's authentication type is `external`, Polaris's own token endpoint is deactivated, regardless of the
credentials used (here, the `root` principal created to initialize the realm's metastore; see the docker-compose
file):

<!-- the '|| true' is there to let Guides CI not fail on this command -->
```shell
curl -v http://localhost:8181/api/catalog/v1/oauth/tokens \
  --user root:s3cr3t \
  -d 'grant_type=client_credentials' \
  -d 'scope=PRINCIPAL_ROLE:ALL' || true
```

This returns a `501 Not Implemented` error. You must request a token from Keycloak instead, on behalf of the
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

This token is valid for 1 hour. It authenticates and authorizes a principal that does not, and does not need to,
exist in the Polaris metastore.

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

This succeeds even though no principal named `keycloak-admin` was ever created in the Polaris metastore: OPA
authorizes the request purely from the `service_admin` role carried in the token.

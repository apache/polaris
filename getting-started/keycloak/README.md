<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
 
   http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Getting Started with Apache Polaris, External Authentication and Keycloak

## Overview

This example uses Keycloak as an **external** identity provider for Polaris. The "iceberg" realm is automatically
created and configured from the `iceberg-realm.json` file.

This Keycloak realm contains 1 client definition: `client1:s3cr3t`. It is configured to return tokens with the following
fixed claims:

- `principal_id`: the principal ID of the user. It is always set to zero (0) in this example.
- `principal_name`: the principal name of the user. It is always set to "root" in this example.
- `principal_roles`: the principal roles of the user. It is always set to `["server_admin", "catalog_admin"]` in this 
  example.

This is obviously not a realistic configuration. In a real-world scenario, you would configure Keycloak to return the
actual principal ID, name and roles of the user. Note that principals and principal roles must have been created in
Polaris beforehand, and the principal ID, name and roles must match the ones returned by Keycloak.

Polaris is configured with 3 realms:

- `realm-internal`: This is the default realm, and is configured to use the internal authentication only. It accepts
  token issues by Polaris itself only.
- `realm-external`: This realm is configured to use an external identity provider (IDP) for authentication only. It
  accepts tokens issued by Keycloak only.
- `realm-mixed`: This realm is configured to use both the internal and external authentication. It accepts tokens 
  issued by both Polaris and Keycloak.

For more information about how to configure Polaris with external authentication, see the
[Polaris documentation](https://polaris.apache.org/in-dev/unreleased/external-idp/).

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
    docker compose -f getting-started/keycloak/docker-compose.yml up
    ```

## Requesting a Token

Note: the commands below require `jq` to be installed on your machine.

### From Polaris

You can request a token from Polaris for realms `realm-internal` and `realm-mixed`:

1. Open a terminal and run the following command to request an access token for the `realm-internal` realm:

    ```shell
    polaris_token_realm_internal=$(curl -s http://localhost:8181/api/catalog/v1/oauth/tokens \
      --user root:s3cr3t \
      -H 'Polaris-Realm: realm-internal' \
      -d 'grant_type=client_credentials' \
      -d 'scope=PRINCIPAL_ROLE:ALL' | jq -r .access_token)
    ```
   
    This token is valid only for the `realm-internal` realm.
   
2. Open a terminal and run the following command to request an access token for the `realm-mixed` realm:

    ```shell
    polaris_token_realm_mixed=$(curl -s http://localhost:8181/api/catalog/v1/oauth/tokens \
      --user root:s3cr3t \
      -H 'Polaris-Realm: realm-mixed' \
      -d 'grant_type=client_credentials' \
      -d 'scope=PRINCIPAL_ROLE:ALL' | jq -r .access_token)
    ```
   
    This token is valid only for the `realm-mixed` realm.

Polaris tokens are valid for 1 hour.

Note: if you request a Polaris token for the `realm-external` realm, it will not work because Polaris won't issue tokens
for this realm:

```shell
curl -v http://localhost:8181/api/catalog/v1/oauth/tokens \
  --user root:s3cr3t \
  -H 'Polaris-Realm: realm-external' \
  -d 'grant_type=client_credentials' \
  -d 'scope=PRINCIPAL_ROLE:ALL'
```

This will return a `501 Not Implemented` error because for this realm, the internal token endpoint has been deactivated.

### From Keycloak

You can request a token from Keycloak for the `realm-external` and `realm-mixed` realms:

1. Open a terminal and run the following command to request an access token from Keycloak:

    ```shell
    keycloak_token=$(curl -s http://keycloak:8080/realms/iceberg/protocol/openid-connect/token \
      --resolve keycloak:8080:127.0.0.1 \
      --user client1:s3cr3t \
      -d 'grant_type=client_credentials' | jq -r .access_token)
    ```

Note the `--resolve` option: it is used to send the request with the `Host` header set to `keycloak`. This is necessary
because Keycloak issues tokens with the `iss` claim matching the request's `Host` header; without this, the token would
not be valid when used against Polaris because the `iss` claim would be `127.0.0.1`, but Polaris expects it to be
`keycloak`, since that's Keycloak's hostname within the Docker network.

Tokens issued by Keycloak can be used to access Polaris with the `realm-external` or `realm-mixed` realms. Access tokens
are valid for 1 hour.

You can also access the Keycloak admin console. Open a browser and go to [http://localhost:8080](http://localhost:8080),
then log in with the username `admin` and password `admin` (you can change this in the docker-compose file).

## Accessing Polaris with the Tokens

You can access Polaris using the tokens you obtained above. The following examples show how to use the tokens with
`curl`:

### Using the Polaris Token

1. Open a terminal and run the following command to list the principal roles in the `realm-internal` realm:

    ```shell
    curl -v http://localhost:8181/api/management/v1/catalogs \
      -H "Authorization: Bearer $polaris_token_realm_internal" \
      -H 'Polaris-Realm: realm-internal' \
      -H 'Accept: application/json'
    ```
   
2. Open a terminal and run the following command to list the principal roles in the `realm-mixed` realm:

    ```shell
    curl -v http://localhost:8181/api/management/v1/catalogs \
      -H "Authorization: Bearer $polaris_token_realm_mixed" \
      -H 'Polaris-Realm: realm-mixed' \
      -H 'Accept: application/json'
    ```

Note: you cannot mix tokens from different realms. For example, you cannot use a token from the `realm-internal` realm to access
the `realm-mixed` realm:

```shell
curl -v http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $polaris_token_realm_internal" \
  -H 'Polaris-Realm: realm-mixed' \
  -H 'Accept: application/json'
```

This will return a `401 Unauthorized` error because the token is not valid for the `realm-mixed` realm.

### Using the Keycloak Token

The same Keycloak token can be used to access both the `realm-external` and `realm-mixed` realms, as it is valid for
both (both realms share the same OIDC tenant configuration).

1. Open a terminal and run the following command to list the principal roles in the `realm-external` realm:

    ```shell
    curl -v http://localhost:8181/api/management/v1/catalogs \
      -H "Authorization: Bearer $keycloak_token" \
      -H 'Polaris-Realm: realm-external' \
      -H 'Accept: application/json'
    ```
   
2. Open a terminal and run the following command to list the principal roles in the `realm-mixed` realm:

    ```shell
    curl -v http://localhost:8181/api/management/v1/catalogs \
      -H "Authorization: Bearer $keycloak_token" \
      -H 'Polaris-Realm: realm-mixed' \
      -H 'Accept: application/json'
    ```

Note: you cannot use a Keycloak token to access the `realm-internal` realm:

```shell
curl -v http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $keycloak_token" \
  -H 'Polaris-Realm: realm-internal' \
  -H 'Accept: application/json'
```

This will return a `401 Unauthorized` error because the token is not valid for the `realm-internal` realm.
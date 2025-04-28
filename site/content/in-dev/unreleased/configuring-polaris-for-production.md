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
title: Configuring Apache Polaris (Incubating) for Production
linkTitle: Deploying In Production
type: docs
weight: 600
---

## Configuring Polaris for Production

The default server configuration is intended for development and testing. When deploying Polaris in
production, there are several best practices to keep in mind.

Notable configuration used to secure a Polaris deployment are outlined below.

For more information on how to configure Polaris and what configuration options are available,
refer to the [configuration reference page]({{% ref "configuration" %}}).

### OAuth2

Polaris authentication requires specifying a token broker factory type. Two implementations are
supported out of the box:

- [rsa-key-pair] uses a pair of public and private keys;
- [symmetric-key] uses a shared secret.

[rsa-key-pair]: https://github.com/apache/polaris/blob/390f1fa57bb1af24a21aa95fdbff49a46e31add7/service/common/src/main/java/org/apache/polaris/service/auth/JWTRSAKeyPairFactory.java
[symmetric-key]: https://github.com/apache/polaris/blob/390f1fa57bb1af24a21aa95fdbff49a46e31add7/service/common/src/main/java/org/apache/polaris/service/auth/JWTSymmetricKeyFactory.java

By default, Polaris uses `rsa-key-pair`, with randomly generated keys.

> [!IMPORTANT]
> The default `rsa-key-pair` configuration is not suitable when deploying many replicas of Polaris,
> as each replica will have its own set of keys. This will cause token validation to fail when a
> request is routed to a different replica than the one that issued the token.

It is highly recommended to configure Polaris with previously-generated RSA keys. This can be done
by setting the following properties:

```properties
polaris.authentication.token-broker.type=rsa-key-pair
polaris.authentication.token-broker.rsa-key-pair.public-key-file=/tmp/public.key
polaris.authentication.token-broker.rsa-key-pair.private-key-file=/tmp/private.key
```

To generate an RSA key pair, you can use the following commands:

```shell
openssl genrsa -out private.key 2048
openssl rsa -in private.key -pubout -out public.key
```

Alternatively, you can use a symmetric key by setting the following properties:

```properties
polaris.authentication.token-broker.type=symmetric-key
polaris.authentication.token-broker.symmetric-key.file=/tmp/symmetric.key
```

Note: it is also possible to set the symmetric key secret directly in the configuration file. If
possible, pass the secret as an environment variable to avoid storing sensitive information in the
configuration file:

```properties
polaris.authentication.token-broker.symmetric-key.secret=${POLARIS_SYMMETRIC_KEY_SECRET}
```

Finally, you can also configure the token broker to use a maximum lifespan by setting the following
property:

```properties
polaris.authentication.token-broker.max-token-generation=PT1H
```

Typically, in Kubernetes, you would define the keys as a `Secret` and mount them as files in the
container.

### Realm Context Resolver

By default, Polaris resolves realms based on incoming request headers. You can configure the realm
context resolver by setting the following properties in `application.properties`:

```properties
polaris.realm-context.realms=POLARIS,MY-REALM
polaris.realm-context.header-name=Polaris-Realm
```

Where:

- `realms` is a comma-separated list of allowed realms. This setting _must_ be correctly configured.
  At least one realm must be specified.
- `header-name` is the name of the header used to resolve the realm; by default, it is
  `Polaris-Realm`.

If a request contains the specified header, Polaris will use the realm specified in the header. If
the realm is not in the list of allowed realms, Polaris will return a `404 Not Found` response.

If a request _does not_ contain the specified header, however, by default Polaris will use the first
realm in the list as the default realm. In the above example, `POLARIS` is the default realm and
would be used if the `Polaris-Realm` header is not present in the request.

This is not recommended for production use, as it may lead to security vulnerabilities. To avoid
this, set the following property to `true`:

```properties
polaris.realm-context.require-header=true
```

This will cause Polaris to also return a `404 Not Found` response if the realm header is not present
in the request.

### Metastore Configuration

A metastore should be configured with an implementation that durably persists Polaris entities. By
default, Polaris uses an in-memory metastore.

> [!IMPORTANT]
> The default in-memory metastore is not suitable for production use, as it will lose all data
> when the server is restarted; it is also unusable when multiple Polaris replicas are used.

To use a durable metastore, you need to switch to the EclipseLink metastore, and provide your own
`persistence.xml` file. This file contains details of the database used for metastore management and
the connection settings. For more information, refer to the [metastore documentation]({{% ref
"metastores" %}}).

Then, configure Polaris to use your metastore by setting the following properties:

```properties
polaris.persistence.type=eclipse-link
polaris.persistence.eclipselink.configuration-file=/path/to/persistence.xml
polaris.persistence.eclipselink.persistence-unit=polaris
```

Where:

- `polaris.persistence.type` indicates that we are using the EclipseLink metastore.
- `polaris.persistence.eclipselink.configuration-file` is the path to the `persistence.xml` file.
- `polaris.persistence.eclipselink.persistence-unit` is the name of the persistence unit to use (in
  case the configuration file has many persistence units).

Typically, in Kubernetes, you would define the `persistence.xml` file as a `ConfigMap` and set the
`polaris.persistence.eclipselink.configuration-file` property to the path of the mounted file in
the container.

> [!IMPORTANT]
> Be sure to secure your metastore backend since it will be storing sensitive data and catalog
> metadata.

### Bootstrapping

Before using Polaris, you must **bootstrap** the metastore. This is a manual operation that must be
performed **only once** for each realm in order to prepare the metastore to integrate with Polaris.

By default, when bootstrapping a new realm, Polaris will create randomised `CLIENT_ID` and
`CLIENT_SECRET` for the `root` principal and store their hashes in the metastore backend.

Depending on your database, this may not be convenient as the generated credentials are not stored
in clear text in the database.

In order to provide your own credentials for `root` principal (so you can request tokens via
`api/catalog/v1/oauth/tokens`), use the [Polaris Admin Tool]({{% ref "admin-tool" %}})

You can verify the setup by attempting a token issue for the `root` principal:

```bash
curl -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -d "grant_type=client_credentials" \
  -d "client_id=my-client-id" \
  -d "client_secret=my-client-secret" \
  -d "scope=PRINCIPAL_ROLE:ALL"
```

Which should return an access token:

```json
{
  "access_token": "...",
  "token_type": "bearer",
  "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
  "expires_in": 3600
}
```

If you used a non-default realm name, add the appropriate request header to the `curl` command,
otherwise Polaris will resolve the realm to the first one in the configuration
`polaris.realm-context.realms`. Here is an example to set realm header:

```bash
curl -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -H "Polaris-Realm: my-realm" \
  -d "grant_type=client_credentials" \
  -d "client_id=my-client-id" \
  -d "client_secret=my-client-secret" \
  -d "scope=PRINCIPAL_ROLE:ALL"
```

## Other Configurations

When deploying Polaris in production, consider adjusting the following configurations:

#### `polaris.features.defaults."SUPPORTED_CATALOG_STORAGE_TYPES"`

- By default, Polaris catalogs are allowed to be located in local filesystem with the `FILE` storage
  type. This should be disabled for production systems.
- Use this configuration to additionally disable any other storage types that will not be in use.


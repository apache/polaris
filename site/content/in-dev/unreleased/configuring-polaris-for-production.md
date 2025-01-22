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

## Tuning Polaris for Production

The default server configuration is intended for development and testing. When deploying Polaris in
production, there are several best practices to keep in mind.

Notable configuration options used to secure a Polaris deployment are outlined below.

For more information on how to configure Polaris and what configuration options are available,
refer to the Configuration Reference page.

### Security

Notable configuration options used to secure a Polaris deployment are outlined below.

Polaris authentication requires specifying a token broker factory type. Two types are supported:

- `rsa-key-pair` uses a pair of public and private keys;
- `symmetric-key` uses a shared secret.

By default, Polaris uses `rsa-key-pair`, with randomly generated keys. 

> [!WARNING]  
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

Alternatively, you can use a symmetric key by setting the following properties:

```properties
polaris.authentication.token-broker.type=symmetric-key
polaris.authentication.token-broker.symmetric-key.file=/tmp/symmetric.key
```

Note: it is also possible to set the symmetric key secret directly in the configuration file, but
that is not recommended for production use, as the secret is stored in plain text:

```properties
polaris.authentication.token-broker.symmetric-key.secret=my-secret
```

Finally, you can also configure the token broker to use a maximum lifespan by setting the following
property:

```properties
polaris.authentication.token-broker.max-token-generation=PT1H
```

### Realm Id Resolver

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

If a request does not contain the specified header, Polaris will use the first realm in the list as
the default realm. In the above example, `POLARIS` is the default realm.

### Metastore Configuration

A Metastore Manager should be configured with an implementation that durably persists Polaris
entities. By default, Polaris uses an in-memory metastore.

> [!WARNING]
> The default in-memory metastore is not suitable for production use, as it will lose all data
> when the server is restarted; it is also unusable when multiple Polaris replicas are used.

To use a durable Metastore Manager, you need to switch to the EclipseLink metastore, and provide
your own `persistence.xml` file. This file contains details of the database used for metastore
management and the connection settings. For more information, refer to the [metastore
documentation]({{% ref "metastores" %}}).

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

Be sure to secure your metastore backend since it will be storing credentials and catalog metadata.

### Bootstrapping

Before using Polaris, you must **bootstrap** the metastore manager. This is a manual operation that
must be performed **only once** in order to prepare the metastore manager to integrate with Polaris.

By default, when bootstrapping a new realm, Polaris will create randomised `CLIENT_ID` and
`CLIENT_SECRET` for the `root` principal and store their hashes in the metastore backend.

Depending on your database, this may not be convenient as the generated credentials are not stored
in clear text in the database.

In order to provide your own credentials for `root` principal (so you can request tokens via
`api/catalog/v1/oauth/tokens`), there are two approaches:

1. Use the Polaris Admin Tool to bootstrap the realm and set its `root`
   principal credentials.
2. Set the `root` principal credentials when deploying Polaris for the first time.

The first approach is recommended as it is more flexible (you can bootstrap new realms at any time
without incurring in a Polaris downtime). The second approach is useful for testing and development,
but can be used in production as well.

In order to bootstrap root credentials for a realm named `my-realm` when deploying Polaris, set the
following environment variables:

```
export POLARIS_BOOTSTRAP_CREDENTIALS=my-realm,my-client-id,my-client-secret
```

If the realm hasn't been bootstrapped yet, Polaris will create the realm and the `root` principal
with the provided credentials upon first usage of that realm. If the realm already exists, Polaris
will not attempt to update the `root` principal credentials.

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

Note that if you used a realm name that is not the default realm name, then you should add an
appropriate request header to the `curl` command, for example:

```bash
curl -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -H "Polaris-Realm: my-realm" \
  -d "grant_type=client_credentials" \
  -d "client_id=my-client-id" \
  -d "client_secret=my-client-secret" \
  -d "scope=PRINCIPAL_ROLE:ALL"
```

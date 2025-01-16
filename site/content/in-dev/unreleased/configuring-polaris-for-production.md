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

The default `polaris-server.yml` configuration is intended for development and testing. When deploying Polaris in production, there are several best practices to keep in mind.

## Security

### Configurations

Notable configuration used to secure a Polaris deployment are outlined below.

#### oauth2

> [!WARNING]  
> Ensure that the `tokenBroker` setting reflects the token broker specified in `authenticator` below.

* Configure [OAuth](https://oauth.net/2/) with this setting. Remove the `TestInlineBearerTokenPolarisAuthenticator` option and uncomment the `DefaultPolarisAuthenticator` authenticator option beneath it.
* Then, configure the token broker. You can configure the token broker to use either [asymmetric](https://github.com/apache/polaris/blob/b482617bf8cc508b37dbedf3ebc81a9408160a5e/polaris-service/src/main/java/io/polaris/service/auth/JWTRSAKeyPair.java#L24) or [symmetric](https://github.com/apache/polaris/blob/b482617bf8cc508b37dbedf3ebc81a9408160a5e/polaris-service/src/main/java/io/polaris/service/auth/JWTSymmetricKeyBroker.java#L23) keys.

#### authenticator.tokenBroker

> [!WARNING]  
> Ensure that the `tokenBroker` setting reflects the token broker specified in `oauth2` above.

#### callContextResolver & realmIdResolver
* Use these configurations to specify a service that can resolve a realm from bearer tokens.
* The service(s) used here must implement the relevant interfaces (i.e. [CallContextResolver](https://github.com/apache/polaris/blob/8290019c10290a600e40b35ddb1e2f54bf99e120/polaris-service/src/main/java/io/polaris/service/context/CallContextResolver.java#L27) and [RealmContextResolver](https://github.com/apache/polaris/blob/7ce86f10a68a3b56aed766235c88d6027c0de038/polaris-service/src/main/java/io/polaris/service/context/RealmContextResolver.java)).

## Metastore Management

> [!IMPORTANT]  
> The default `in-memory` implementation for `metastoreManager` is meant for testing and not suitable for production usage. Instead, consider an implementation such as `eclipse-link` which allows you to store metadata in a remote database.

A Metastore Manger should be configured with an implementation that durably persists Polaris entities. Use the configuration `metaStoreManager` to configure a [MetastoreManager](https://github.com/apache/polaris/blob/627dc602eb15a3258dcc32babf8def34cf6de0e9/polaris-core/src/main/java/io/polaris/core/persistence/PolarisMetaStoreManager.java#L47) implementation where Polaris entities will be persisted. 

Be sure to secure your metastore backend since it will be storing credentials and catalog metadata.

### Configuring EclipseLink

To use EclipseLink for metastore management, specify the configuration `metaStoreManager.conf-file` to point to an EclipseLink `persistence.xml` file. This file, local to the Polaris service, contains details of the database used for metastore management and the connection settings. For more information, refer to the [metastore documentation]({{% ref "metastores" %}}).

> [!IMPORTANT]
> EclipseLink requires
> 1. Building the JAR for the EclipseLink extension
> 2. Setting the `eclipseLink` gradle property to `true`.
>
> This can be achieved by setting `eclipseLink=true` in the `gradle.properties` file, or by passing the property explicitly while building all JARs, e.g.: `./gradlew -PeclipseLink=true clean assemble`

### Bootstrapping

Before using Polaris when using a metastore manager other than `in-memory`, you must **bootstrap** the metastore manager. This is a manual operation that must be performed **only once** in order to prepare the metastore manager to integrate with Polaris. When the metastore manager is bootstrapped, any existing Polaris entities in the metastore manager may be **purged**.

By default, Polaris will create randomised `CLIENT_ID` and `CLIENT_SECRET` for the `root` principal and store their hashes in the metastore backend. In order to provide your own credentials for `root` principal (so you can request tokens via `api/catalog/v1/oauth/tokens`), set the `POLARIS_BOOTSTRAP_CREDENTIALS` environment variable as follows:

```
export POLARIS_BOOTSTRAP_CREDENTIALS=my_realm,root,my-client-id,my-client-secret
```

The format of the environment variable is `realm,principal,client_id,client_secret`. You can provide multiple credentials separated by `;`. For example, to provide credentials for two realms `my_realm` and `my_realm2`:

```
export POLARIS_BOOTSTRAP_CREDENTIALS=my_realm,root,my-client-id,my-client-secret;my_realm2,root,my-client-id2,my-client-secret2
```

You can also provide credentials for other users too. 

It is also possible to use system properties to provide the credentials:

```
java -Dpolaris.bootstrap.credentials=my_realm,root,my-client-id,my-client-secret -jar /path/to/jar/polaris-service-all.jar bootstrap polaris-server.yml
```

Now, to bootstrap Polaris, run:

```bash
java -jar /path/to/jar/polaris-service-all.jar bootstrap polaris-server.yml
```

or in a container:

```bash
bin/polaris-service bootstrap config/polaris-server.yml
```

Afterward, Polaris can be launched normally:

```bash
java -jar /path/to/jar/polaris-service-all.jar server polaris-server.yml
```

You can verify the setup by attempting a token issue for the `root` principal:

```bash
curl -X POST http://localhost:8181/api/catalog/v1/oauth/tokens -d "grant_type=client_credentials&client_id=my-client-id&client_secret=my-client-secret&scope=PRINCIPAL_ROLE:ALL"
```

which should return:

```json
{"access_token":"...","token_type":"bearer","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","expires_in":3600}
```

Note that if you used non-default realm name, for example, `iceberg` instead of `default-realm` in your `polaris-server.yml`, then you should add an appropriate request header:
```bash
curl -X POST -H 'realm: iceberg' http://localhost:8181/api/catalog/v1/oauth/tokens -d "grant_type=client_credentials&client_id=my-client-id&client_secret=my-client-secret&scope=PRINCIPAL_ROLE:ALL"
```

## Other Configurations

When deploying Polaris in production, consider adjusting the following configurations:

#### featureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES
  - By default Polaris catalogs are allowed to be located in local filesystem with the `FILE` storage type. This should be disabled for production systems.
  - Use this configuration to additionally disable any other storage types that will not be in use.



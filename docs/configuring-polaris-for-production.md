<!--
 Copyright (c) 2024 Snowflake Computing Inc.
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->


# Configuring Polaris for Production

The default `polaris-server.yml` configuration is intended for develoment and testing. When deploying Polaris in production, there are several best practices to keep in mind.

## Security

### Configurations

Notable configuration used to secure a Polaris deployment are outlined below.

#### oauth2

> [!WARNING]  
> Ensure that the `tokenBroker` setting reflects the token broker specified in `authenticator` below.

* Configure [OAuth](https://oauth.net/2/) with this setting. Remove the `TestInlineBearerTokenPolarisAuthenticator` option and uncomment the `DefaultPolarisAuthenticator` authenticator option beneath it.
* Then, configure the token broker. You can configure the token broker to use either [asymmetric](https://github.com/polaris-catalog/polaris/blob/b482617bf8cc508b37dbedf3ebc81a9408160a5e/polaris-service/src/main/java/io/polaris/service/auth/JWTRSAKeyPair.java#L24) or [symmetric](https://github.com/polaris-catalog/polaris/blob/b482617bf8cc508b37dbedf3ebc81a9408160a5e/polaris-service/src/main/java/io/polaris/service/auth/JWTSymmetricKeyBroker.java#L23) keys.

#### authenticator.tokenBroker

> [!WARNING]  
> Ensure that the `tokenBroker` setting reflects the token broker specified in `oauth2` above.

#### callContextResolver & realmContextResolver
* Use these configurations to specify a service that can resolve a realm from bearer tokens.
* The service(s) used here must implement the relevant interfaces (i.e. [CallContextResolver](https://github.com/polaris-catalog/polaris/blob/8290019c10290a600e40b35ddb1e2f54bf99e120/polaris-service/src/main/java/io/polaris/service/context/CallContextResolver.java#L27) and [RealmContextResolver](https://github.com/polaris-catalog/polaris/blob/7ce86f10a68a3b56aed766235c88d6027c0de038/polaris-service/src/main/java/io/polaris/service/context/RealmContextResolver.java)).

## Metastore Management

> [!IMPORTANT]  
> The default `in-memory` implementation for `metastoreManager` is meant for testing and not suitable for production usage. Instead, consider an implementation such as `eclipse-link` which allows you to store metadata in a remote database.

A Metastore Manger should be configured with an implementation that durably persists Polaris entities. Use the configuration `metaStoreManager` to configure a [MetastoreManager](https://github.com/polaris-catalog/polaris/blob/627dc602eb15a3258dcc32babf8def34cf6de0e9/polaris-core/src/main/java/io/polaris/core/persistence/PolarisMetaStoreManager.java#L47) implementation where Polaris entities will be persisted. 

Be sure to secure your metastore backend since it will be storing credentials and catalog metadata.

### Configuring EclipseLink

To use [EclipseLink](https://eclipse.dev/eclipselink/) for metastore management, specify the configuration `metaStoreManager.conf-file` to point to an [EclipseLink `persistence.xml` file](https://eclipse.dev/eclipselink/documentation/2.5/solutions/testingjpa002.htm). This file, local to the Polaris service, will contain information on what database to use for metastore management and how to connect to it.

### Bootstrapping

Before using Polaris when using a metastore manager other than `in-memory`, you must **bootstrap** the metastore manager. This is a manual operation that must be performed **only once** in order to prepare the metastore manager to integrate with Polaris. When the metastore manager is bootstrapped, any existing Polaris entities in the metastore manager may be **purged**.

To bootstrap Polaris, run:

```bash
java -jar /path/to/jar/polaris-service-all.jar bootstrap polaris-server.yml
```

Afterwards, Polaris can be launched normally:

```bash
java -jar /path/to/jar/polaris-service-all.jar server polaris-server.yml
```

## Other Configurations

When deploying Polaris in production, consider adjusting the following configurations:

#### featureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES
  - By default Polaris catalogs are allowed to be located in local filesystem with the `FILE` storage type. This should be disabled for production systems.
  - Use this configuration to additionally disable any other storage types that will not be in use.



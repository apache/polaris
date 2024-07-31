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

There are many Polaris configurations that should be adjusted to ensure a secure Polaris deployment. Some of these configurations are briefly outlined below, along with a short description of each.

* **oauth2**
  - Configure [OAuth](https://oauth.net/2/) with this setting, including a token broker

* **callContextResolver** & **realmContextResolver**
  - Use these configurations to specify a service that can resolve a realm from your bearer tokens.
  - The service(s) used here must implement the relevant interfaces (e.g. [CallContextResolver](https://github.com/polaris-catalog/polaris/blob/8290019c10290a600e40b35ddb1e2f54bf99e120/polaris-service/src/main/java/io/polaris/service/context/CallContextResolver.java#L27)).

* **authenticator.tokenBroker**
  - Ensure that this setting reflects the token broker specified in **oauth2** above

* **cors**
  - Use settings like **cors.allowed-origins** to control what connections your Polaris service will accept


## Metastore Management

Use the configuration `metaStoreManager` to configure a [MetastoreManager](https://github.com/polaris-catalog/polaris/blob/627dc602eb15a3258dcc32babf8def34cf6de0e9/polaris-core/src/main/java/io/polaris/core/persistence/PolarisMetaStoreManager.java#L47) implementation where Polaris entities will be persisted. 

The default `in-memory` implementation is meant for testing and not suitable for production usage. Instead, consider an implementation such as `eclipse-link` which allows you to store metadata in a remote database.

Credentials and metadata will be stored in the metastore manager, and so be sure that your metastore manager is accordingly secured.

### Configuring EclipseLink

To use [EclipseLink](https://eclipse.dev/eclipselink/) for metastore management, specify the configuration `metaStoreManager.conf-file` to point to an [EclipseLink `persistence.xml` file](https://eclipse.dev/eclipselink/documentation/2.5/solutions/testingjpa002.htm). This file, local to the Polaris service, will contain information on what database to use for metastore management and how to connect to it.

## Other Configurations

When deploying Polaris in production, consider adjusting the following configurations:

* **featureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES**
  - By default, the `FILE` storage type may be supported. This is intended for testing, and in produciton you'll likely want to disable it
  - Here you can also disable or enable any other storage type based on your expected usage of Apache Iceberg



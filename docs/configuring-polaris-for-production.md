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



## Other Configurations

When deploying Polaris in production, consider adjusting the following configurations:

* **featureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES**
  - By default, the `FILE` storage type may be supported. This is intended for testing, and in produciton you'll likely want to disable it
  - Here you can also disable or enable any other storage type based on your expected usage of Apache Iceberg



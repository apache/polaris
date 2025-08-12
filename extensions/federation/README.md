### Using the `HadoopFederatedCatalogFactory`

This `HadoopFederatedCatalogFactory` module is an independent compilation unit and will be built into the Polaris binary only when the following flag is set in the gradle.properties file:
```
NonRESTCatalogs=HADOOP,<alternates>
```

The other option is to pass it as an argument to the gradle JVM as follows: 
```
./gradlew build -DNonRESTCatalogs=HADOOP
```

Without this flag, the Hadoop factory won't be compiled into Polaris and therefore Polaris will not load the class at runtime, throwing an unsupported exception for federated catalog calls.
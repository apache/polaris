### Using the `HadoopFederatedCatalogFactory`

This module is an independent compilation unit and will be built into the Polaris binary only when the following flag is set in gradle.properties or as a JVM arg at compile time:

```
NonRESTCatalogs=HADOOP,<alternates>
```

Without this flag, the Hadoop factory won't be compiled into Polaris and therefore Polaris will not load the class at runtime, throwing an unsupported exception for federated catalog calls.
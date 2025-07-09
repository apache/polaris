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

This module contains the Polaris Service powered by Quarkus.

# Main differences

* Bean injection (CDI) is made using `@ApplicationScoped` annotation on class and injected in other classes using `@Inject` annotation (https://quarkus.io/guides/cdi-reference). 
* Codehale metrics registry and opentelemetry boilerplate (prometheus exporter included) are not needed anymore: Quarkus provides it "out of the box" (https://quarkus.io/guides/opentelemetry)
* `PolarisHealthCheck` is not needed anymore: Quarkus provides it "out of the box" (https://quarkus.io/guides/smallrye-health)
* `TimedApplicationEventListener` and the `@TimedApi` annotation are replaced by Quarkus (micrometer) `@Timed` annotation (https://quarkus.io/guides/telemetry-micrometer)
* `PolarisJsonLayoutFactory` is not needed anymore: Quarkus provides it by configuration (using `quarkus.log.*` configuration)
* `PolarisApplication` is not needed, Quarkus provide a "main" application out of the box (it's possible to provide `QuarkusApplication` for control the startup and also using `@Startup` annotation)
* CORS boilerplate is not needed anymore: Quarkus supports it via configuration (using `quarkus.http.cors.*` configuration)
* CLI is not part of `polaris-service` anymore, we have (will have) a dedicated module (`polaris-cli`)

# Build and run

To build `polaris-service` you simply do:

```
./gradlew :polaris-service:build
```

The build creates ready to run package:
* in the `build/quarkus-app` folder, you can run with `java -jar quarkus-run.jar`
* the `build/distributions` folder contains tar/zip distributions you can extract  

You can directly run Polaris service (in the build scope) using:

```
./gradlew :polaris-runtime-service:quarkusRun
```

You can run in Dev mode as well:

```
./gradlew --console=plain :polaris-runtime-service:quarkusDev
```

You can directly build a Docker image using:

```
./gradlew :polaris-runtime-service:imageBuild
```

# Configuration

The main configuration file is not the `application.properties`. The default configuration is
packaged as part of the `polaris-runtime-service`. `polaris-runtime-service` uses several 
configuration sources (in this order):
* system properties
* environment variables
* `.env` file in the current working directory
* `$PWD/config/application.properties` file
* the `application.properties` packaged in the `polaris-runtime-service` application

It means you can override some configuration property using environment variables for example.

By default, `polaris-runtime-service` uses 8181 as the HTTP port (defined in the `quarkus.http.port`
configuration property) and 8182 as the management port (defined in the `quarkus.management.port`
configuration property).

You can find more details here: https://quarkus.io/guides/config

# Integration tests
Integration tests from the :polaris-tests module can be run against a local Polaris Quarkus instance
for each supported cloud storage. Set the appropriate environment variables for your target cloud,
then run the tests as shown below.

For S3:
```shell
export INTEGRATION_TEST_S3_PATH="s3://bucket/subpath"
export INTEGRATION_TEST_S3_ROLE_ARN="your-role-arn"
./gradlew :polaris-runtime-service:intTest
```
For Azure:
```shell
export INTEGRATION_TEST_AZURE_PATH="abfss://bucket/subpath"
export INTEGRATION_TEST_AZURE_TENANT_ID="your-tenant-id"
./gradlew :polaris-runtime-service:intTest
``` 
For GCS:
```shell
export INTEGRATION_TEST_GCS_PATH="gs://bucket/subpath"
export INTEGRATION_TEST_GCS_SERVICE_ACCOUNT="your-service-account"
./gradlew :polaris-runtime-service:intTest
```

# TODO

* Modify `CallContext` and remove all usages of `ThreadLocal`, replace with proper context propagation.
* Remove `PolarisCallContext` – it's just an aggregation of CDI beans
* Use `@QuarkustIntegrationTest` for integration tests
* Remove `OAuthCredentialAuthFilter` and replace with Quarkus OIDC security
* Create `polaris-cli` module, add Bootstrap and Purge commands
* Adapt Helm charts, Dockerfiles, K8s examples
* Fix intermittent Gradle build error : SRCFG00011: Could not expand value
  platform.quarkus.native.builder-image in property quarkus.native.builder-image
  (https://github.com/quarkusio/quarkus/issues/19139).
  The Quarkus issue says that using the Quarkus-platform bom helps (it's really what we should depend on). But   IIRC there were some dependency issues with Spark/Scala, which prevents us from using the Quarkus-platform bom.

* Update documentation/README/...

* Do we want to support existing json configuration file as configuration source ?

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
Title: Polaris Helm Chart
type: docs
weight: 675
---

<!---
  This README.md file was generated with:
  https://github.com/norwoodj/helm-docs
  Do not modify the README.md file directly, please modify README.md.gotmpl instead.
  To re-generate the README.md file, install helm-docs then run from the repo root:
  helm-docs --chart-search-root=helm
-->

![Version: 1.2.0-incubating-SNAPSHOT](https://img.shields.io/badge/Version-1.2.0--incubating--SNAPSHOT-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 1.2.0-incubating-SNAPSHOT](https://img.shields.io/badge/AppVersion-1.2.0--incubating--SNAPSHOT-informational?style=flat-square)

A Helm chart for Apache Polaris (incubating).

**Homepage:** <https://polaris.apache.org/>

## Source Code

* <https://github.com/apache/polaris>

## Installation

### Running locally with a Minikube cluster

The below instructions assume Minikube and Helm are installed.

Start the Minikube cluster, build and load image into the Minikube cluster:

```bash
minikube start
eval $(minikube docker-env)

./gradlew \
  :polaris-server:assemble \
  :polaris-server:quarkusAppPartsBuild --rerun \
  :polaris-admin:assemble \
  :polaris-admin:quarkusAppPartsBuild --rerun \
  -Dquarkus.container-image.build=true
```

### Installing the chart locally

The below instructions assume a local Kubernetes cluster is running and Helm is installed.

#### Common setup

Create the target namespace:
```bash
kubectl create namespace polaris
```

Create all the required resources in the `polaris` namespace. This usually includes a Postgres
database, Kubernetes secrets, and service accounts. The Polaris chart does not create
these resources automatically, as they are not required for all Polaris deployments. The chart will
fail if these resources are not created beforehand. You can find some examples in the
`helm/polaris/ci/fixtures` directory, but beware that these are primarily intended for tests.

Below are two sample deployment models for installing the chart: one with a non-persistent backend and another with a persistent backend.

> [!WARNING]
> The examples below use values files located in the `helm/polaris/ci` directory.
> **These files are intended for testing purposes primarily, and may not be suitable for production use**.
> For production deployments, create your own values files based on the provided examples.

#### Non-persistent backend

Install the chart with a non-persistent backend. From Polaris repo root:
```bash
helm upgrade --install --namespace polaris \
  polaris helm/polaris
```

#### Persistent backend

> [!WARNING]
> The Postgres deployment set up in the fixtures directory is intended for testing purposes only and is not suitable for production use. For production deployments, use a managed Postgres service or a properly configured and secured Postgres instance.

Install the chart with a persistent backend. From Polaris repo root:
```bash
helm upgrade --install --namespace polaris \
  --values helm/polaris/ci/persistence-values.yaml \
  polaris helm/polaris
kubectl wait --namespace polaris --for=condition=ready pod --selector=app.kubernetes.io/name=polaris --timeout=120s
```

To access Polaris and Postgres locally, set up port forwarding for both services (This is needed for bootstrap processes):
```bash
kubectl port-forward -n polaris $(kubectl get pod -n polaris -l app.kubernetes.io/name=polaris -o jsonpath='{.items[0].metadata.name}') 8181:8181

kubectl port-forward -n polaris $(kubectl get pod -n polaris -l app.kubernetes.io/name=postgres -o jsonpath='{.items[0].metadata.name}') 5432:5432
```

Run the catalog bootstrap using the Polaris admin tool. This step initializes the catalog with the required configuration:
```bash
container_envs=$(kubectl exec -it -n polaris $(kubectl get pod -n polaris -l app.kubernetes.io/name=polaris -o jsonpath='{.items[0].metadata.name}') -- env)
export QUARKUS_DATASOURCE_USERNAME=$(echo "$container_envs" | grep quarkus.datasource.username | awk -F '=' '{print $2}' | tr -d '\n\r')
export QUARKUS_DATASOURCE_PASSWORD=$(echo "$container_envs" | grep quarkus.datasource.password | awk -F '=' '{print $2}' | tr -d '\n\r')
export QUARKUS_DATASOURCE_JDBC_URL=$(echo "$container_envs" | grep quarkus.datasource.jdbc.url | sed 's/postgres/localhost/2' | awk -F '=' '{print $2}' | tr -d '\n\r')

java -jar runtime/admin/build/quarkus-app/quarkus-run.jar bootstrap -c POLARIS,root,pass -r POLARIS
```

### Uninstalling

```bash
helm uninstall --namespace polaris polaris

kubectl delete --namespace polaris -f helm/polaris/ci/fixtures/

kubectl delete namespace polaris
```

## Development & Testing

This section is intended for developers who want to run the Polaris Helm chart tests.

### Prerequisites

The following tools are required to run the tests:

* [Helm Unit Test](https://github.com/helm-unittest/helm-unittest)
* [Chart Testing](https://github.com/helm/chart-testing)

Quick installation instructions for these tools:
```bash
helm plugin install https://github.com/helm-unittest/helm-unittest.git
brew install chart-testing
```

The integration tests also require some fixtures to be deployed. The `ci/fixtures` directory
contains the required resources. To deploy them, run the following command:
```bash
kubectl apply --namespace polaris -f helm/polaris/ci/fixtures/
kubectl wait --namespace polaris --for=condition=ready pod --selector=app.kubernetes.io/name=postgres --timeout=120s
```

The `helm/polaris/ci` contains a number of values files that will be used to install the chart with
different configurations.

### Running the unit tests

Helm unit tests do not require a Kubernetes cluster. To run the unit tests, execute Helm Unit from
the Polaris repo root:
```bash
helm unittest helm/polaris
```

You can also lint the chart using the Chart Testing tool, with the following command:

```bash
ct lint --charts helm/polaris
```

### Running the integration tests

Integration tests require a Kubernetes cluster. See installation instructions above for setting up
a local cluster.

Integration tests are run with the Chart Testing tool:
```bash
ct install --namespace polaris --charts ./helm/polaris
```

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| advancedConfig | object | `{}` | Advanced configuration. You can pass here any valid Polaris or Quarkus configuration property. Any property that is defined here takes precedence over all the other configuration values generated by this chart. Properties can be passed "flattened" or as nested YAML objects (see examples below). Note: values should be strings; avoid using numbers, booleans, or other types. |
| affinity | object | `{}` | Affinity and anti-affinity for polaris pods. See https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity. |
| authentication | object | `{"authenticator":{"type":"default"},"realmOverrides":{},"tokenBroker":{"maxTokenGeneration":"PT1H","secret":{"name":null,"privateKey":"private.pem","publicKey":"public.pem","rsaKeyPair":{"privateKey":"private.pem","publicKey":"public.pem"},"secretKey":"symmetric.pem","symmetricKey":{"secretKey":"symmetric.key"}},"type":"rsa-key-pair"},"tokenService":{"type":"default"},"type":"internal"}` | Polaris authentication configuration. |
| authentication.authenticator | object | `{"type":"default"}` | The `Authenticator` implementation to use. Only one built-in type is supported: default. |
| authentication.realmOverrides | object | `{}` | Authentication configuration overrides per realm. |
| authentication.tokenBroker | object | `{"maxTokenGeneration":"PT1H","secret":{"name":null,"privateKey":"private.pem","publicKey":"public.pem","rsaKeyPair":{"privateKey":"private.pem","publicKey":"public.pem"},"secretKey":"symmetric.pem","symmetricKey":{"secretKey":"symmetric.key"}},"type":"rsa-key-pair"}` | The `TokenBroker` implementation to use. Two built-in types are supported: rsa-key-pair and symmetric-key. Only relevant when using internal (or mixed) authentication. When using external authentication, the token broker is not used. |
| authentication.tokenBroker.maxTokenGeneration | string | `"PT1H"` | Maximum token generation duration (e.g., PT1H for 1 hour). |
| authentication.tokenBroker.secret | object | `{"name":null,"privateKey":"private.pem","publicKey":"public.pem","rsaKeyPair":{"privateKey":"private.pem","publicKey":"public.pem"},"secretKey":"symmetric.pem","symmetricKey":{"secretKey":"symmetric.key"}}` | The secret name to pull the public and private keys, or the symmetric key secret from. |
| authentication.tokenBroker.secret.name | string | `nil` | The name of the secret to pull the keys from. If not provided, a key pair will be generated. This is not recommended for production. |
| authentication.tokenBroker.secret.privateKey | string | `"private.pem"` | DEPRECATED: Use `authentication.tokenBroker.secret.rsaKeyPair.privateKey` instead. Key name inside the secret for the private key |
| authentication.tokenBroker.secret.publicKey | string | `"public.pem"` | DEPRECATED: Use `authentication.tokenBroker.secret.rsaKeyPair.publicKey` instead. Key name inside the secret for the public key |
| authentication.tokenBroker.secret.rsaKeyPair | object | `{"privateKey":"private.pem","publicKey":"public.pem"}` | Optional: configuration specific to RSA key pair secret. |
| authentication.tokenBroker.secret.rsaKeyPair.privateKey | string | `"private.pem"` | Key name inside the secret for the private key |
| authentication.tokenBroker.secret.rsaKeyPair.publicKey | string | `"public.pem"` | Key name inside the secret for the public key |
| authentication.tokenBroker.secret.secretKey | string | `"symmetric.pem"` | DEPRECATED: Use `authentication.tokenBroker.secret.symmetricKey.secretKey` instead. Key name inside the secret for the symmetric key |
| authentication.tokenBroker.secret.symmetricKey | object | `{"secretKey":"symmetric.key"}` | Optional: configuration specific to symmetric key secret. |
| authentication.tokenBroker.secret.symmetricKey.secretKey | string | `"symmetric.key"` | Key name inside the secret for the symmetric key |
| authentication.tokenService | object | `{"type":"default"}` | The token service (`IcebergRestOAuth2ApiService`) implementation to use. Two built-in types are supported: default and disabled. Only relevant when using internal (or mixed) authentication. When using external authentication, the token service is always disabled. |
| authentication.type | string | `"internal"` | The type of authentication to use. Three built-in types are supported: internal, external, and mixed. |
| autoscaling.enabled | bool | `false` | Specifies whether automatic horizontal scaling should be enabled. Do not enable this when using in-memory version store type. |
| autoscaling.maxReplicas | int | `3` | The maximum number of replicas to maintain. |
| autoscaling.minReplicas | int | `1` | The minimum number of replicas to maintain. |
| autoscaling.targetCPUUtilizationPercentage | int | `80` | Optional; set to zero or empty to disable. |
| autoscaling.targetMemoryUtilizationPercentage | string | `nil` | Optional; set to zero or empty to disable. |
| configMapLabels | object | `{}` | Additional Labels to apply to polaris configmap. |
| containerSecurityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"runAsNonRoot":true,"runAsUser":10000,"seccompProfile":{"type":"RuntimeDefault"}}` | Security context for the polaris container. See https://kubernetes.io/docs/tasks/configure-pod-container/security-context/. |
| containerSecurityContext.runAsUser | int | `10000` | UID 10000 is compatible with Polaris OSS default images; change this if you are using a different image. |
| cors | object | `{"accessControlAllowCredentials":null,"accessControlMaxAge":null,"allowedHeaders":[],"allowedMethods":[],"allowedOrigins":[],"exposedHeaders":[]}` | Polaris CORS configuration. |
| cors.accessControlAllowCredentials | string | `nil` | The `Access-Control-Allow-Credentials` response header. The value of this header will default to `true` if `allowedOrigins` property is set and there is a match with the precise `Origin` header. |
| cors.accessControlMaxAge | string | `nil` | The `Access-Control-Max-Age` response header value indicating how long the results of a pre-flight request can be cached. Must be a valid duration. |
| cors.allowedHeaders | list | `[]` | HTTP headers allowed for CORS, ex: X-Custom, Content-Disposition. If this is not set or empty, all requested headers are considered allowed. |
| cors.allowedMethods | list | `[]` | HTTP methods allowed for CORS, ex: GET, PUT, POST. If this is not set or empty, all requested methods are considered allowed. |
| cors.allowedOrigins | list | `[]` | Origins allowed for CORS, e.g. http://polaris.apache.org, http://localhost:8181. In case an entry of the list is surrounded by forward slashes, it is interpreted as a regular expression. |
| cors.exposedHeaders | list | `[]` | HTTP headers exposed to the client, ex: X-Custom, Content-Disposition. The default is an empty list. |
| extraEnv | list | `[]` | Advanced configuration via Environment Variables. Extra environment variables to add to the Polaris server container. You can pass here any valid EnvVar object: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.27/#envvar-v1-core This can be useful to get configuration values from Kubernetes secrets or config maps. |
| extraInitContainers | list | `[]` | Add additional init containers to the polaris pod(s) See https://kubernetes.io/docs/concepts/workloads/pods/init-containers/. |
| extraServices | list | `[]` | Additional service definitions. All service definitions always select all Polaris pods. Use this if you need to expose specific ports with different configurations, e.g. expose polaris-http with an alternate LoadBalancer service instead of ClusterIP. |
| extraVolumeMounts | list | `[]` | Extra volume mounts to add to the polaris container. See https://kubernetes.io/docs/concepts/storage/volumes/. |
| extraVolumes | list | `[]` | Extra volumes to add to the polaris pod. See https://kubernetes.io/docs/concepts/storage/volumes/. |
| features | object | `{"realmOverrides":{}}` | Polaris features configuration. |
| features.realmOverrides | object | `{}` | Features to enable or disable per realm. This field is a map of maps. The realm name is the key, and the value is a map of feature names to values. If a feature is not present in the map, the default value from the 'defaults' field is used. |
| fileIo | object | `{"type":"default"}` | Polaris FileIO configuration. |
| fileIo.type | string | `"default"` | The type of file IO to use. Two built-in types are supported: default and wasb. The wasb one translates WASB paths to ABFS ones. |
| image.configDir | string | `"/deployments/config"` | The path to the directory where the application.properties file, and other configuration files, if any, should be mounted. |
| image.pullPolicy | string | `"IfNotPresent"` | The image pull policy. |
| image.repository | string | `"apache/polaris"` | The image repository to pull from. |
| image.tag | string | `"latest"` | The image tag. |
| imagePullSecrets | list | `[]` | References to secrets in the same namespace to use for pulling any of the images used by this chart. Each entry is a LocalObjectReference to an existing secret in the namespace. The secret must contain a .dockerconfigjson key with a base64-encoded Docker configuration file. See https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/ for more information. |
| ingress.annotations | object | `{}` | Annotations to add to the ingress. |
| ingress.className | string | `""` | Specifies the ingressClassName; leave empty if you don't want to customize it |
| ingress.enabled | bool | `false` | Specifies whether an ingress should be created. |
| ingress.hosts | list | `[{"host":"chart-example.local","paths":[]}]` | A list of host paths used to configure the ingress. |
| ingress.tls | list | `[]` | A list of TLS certificates; each entry has a list of hosts in the certificate, along with the secret name used to terminate TLS traffic on port 443. |
| livenessProbe | object | `{"failureThreshold":3,"initialDelaySeconds":5,"periodSeconds":10,"successThreshold":1,"terminationGracePeriodSeconds":30,"timeoutSeconds":10}` | Configures the liveness probe for polaris pods. |
| livenessProbe.failureThreshold | int | `3` | Minimum consecutive failures for the probe to be considered failed after having succeeded. Minimum value is 1. |
| livenessProbe.initialDelaySeconds | int | `5` | Number of seconds after the container has started before liveness probes are initiated. Minimum value is 0. |
| livenessProbe.periodSeconds | int | `10` | How often (in seconds) to perform the probe. Minimum value is 1. |
| livenessProbe.successThreshold | int | `1` | Minimum consecutive successes for the probe to be considered successful after having failed. Minimum value is 1. |
| livenessProbe.terminationGracePeriodSeconds | int | `30` | Optional duration in seconds the pod needs to terminate gracefully upon probe failure. Minimum value is 1. |
| livenessProbe.timeoutSeconds | int | `10` | Number of seconds after which the probe times out. Minimum value is 1. |
| logging | object | `{"categories":{"org.apache.iceberg.rest":"INFO","org.apache.polaris":"INFO"},"console":{"enabled":true,"format":"%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%c{3.}] [%X{requestId},%X{realmId}] [%X{traceId},%X{parentId},%X{spanId},%X{sampled}] (%t) %s%e%n","json":false,"threshold":"ALL"},"file":{"enabled":false,"fileName":"polaris.log","format":"%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%c{3.}] [%X{requestId},%X{realmId}] [%X{traceId},%X{parentId},%X{spanId},%X{sampled}] (%t) %s%e%n","json":false,"logsDir":"/deployments/logs","rotation":{"fileSuffix":null,"maxBackupIndex":5,"maxFileSize":"100Mi"},"storage":{"className":"standard","selectorLabels":{},"size":"512Gi"},"threshold":"ALL"},"level":"INFO","mdc":{},"requestIdHeaderName":"X-Request-ID"}` | Logging configuration. |
| logging.categories | object | `{"org.apache.iceberg.rest":"INFO","org.apache.polaris":"INFO"}` | Configuration for specific log categories. |
| logging.console | object | `{"enabled":true,"format":"%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%c{3.}] [%X{requestId},%X{realmId}] [%X{traceId},%X{parentId},%X{spanId},%X{sampled}] (%t) %s%e%n","json":false,"threshold":"ALL"}` | Configuration for the console appender. |
| logging.console.enabled | bool | `true` | Whether to enable the console appender. |
| logging.console.format | string | `"%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%c{3.}] [%X{requestId},%X{realmId}] [%X{traceId},%X{parentId},%X{spanId},%X{sampled}] (%t) %s%e%n"` | The log format to use. Ignored if JSON format is enabled. See https://quarkus.io/guides/logging#logging-format for details. |
| logging.console.json | bool | `false` | Whether to log in JSON format. |
| logging.console.threshold | string | `"ALL"` | The log level of the console appender. |
| logging.file | object | `{"enabled":false,"fileName":"polaris.log","format":"%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%c{3.}] [%X{requestId},%X{realmId}] [%X{traceId},%X{parentId},%X{spanId},%X{sampled}] (%t) %s%e%n","json":false,"logsDir":"/deployments/logs","rotation":{"fileSuffix":null,"maxBackupIndex":5,"maxFileSize":"100Mi"},"storage":{"className":"standard","selectorLabels":{},"size":"512Gi"},"threshold":"ALL"}` | Configuration for the file appender. |
| logging.file.enabled | bool | `false` | Whether to enable the file appender. |
| logging.file.fileName | string | `"polaris.log"` | The log file name. |
| logging.file.format | string | `"%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%c{3.}] [%X{requestId},%X{realmId}] [%X{traceId},%X{parentId},%X{spanId},%X{sampled}] (%t) %s%e%n"` | The log format to use. Ignored if JSON format is enabled. See https://quarkus.io/guides/logging#logging-format for details. |
| logging.file.json | bool | `false` | Whether to log in JSON format. |
| logging.file.logsDir | string | `"/deployments/logs"` | The local directory where log files are stored. The persistent volume claim will be mounted here. |
| logging.file.rotation | object | `{"fileSuffix":null,"maxBackupIndex":5,"maxFileSize":"100Mi"}` | Log rotation configuration. |
| logging.file.rotation.fileSuffix | string | `nil` | An optional suffix to append to the rotated log files. If present, the rotated log files will be grouped in time buckets, and each bucket will contain at most maxBackupIndex files. The suffix must be in a date-time format that is understood by DateTimeFormatter. If the suffix ends with .gz or .zip, the rotated files will also be compressed using the corresponding algorithm. |
| logging.file.rotation.maxBackupIndex | int | `5` | The maximum number of backup files to keep. |
| logging.file.rotation.maxFileSize | string | `"100Mi"` | The maximum size of the log file before it is rotated. Should be expressed as a Kubernetes quantity. |
| logging.file.storage | object | `{"className":"standard","selectorLabels":{},"size":"512Gi"}` | The log storage configuration. A persistent volume claim will be created using these settings. |
| logging.file.storage.className | string | `"standard"` | The storage class name of the persistent volume claim to create. |
| logging.file.storage.selectorLabels | object | `{}` | Labels to add to the persistent volume claim spec selector; a persistent volume with matching labels must exist. Leave empty if using dynamic provisioning. |
| logging.file.storage.size | string | `"512Gi"` | The size of the persistent volume claim to create. |
| logging.file.threshold | string | `"ALL"` | The log level of the file appender. |
| logging.level | string | `"INFO"` | The log level of the root category, which is used as the default log level for all categories. |
| logging.mdc | object | `{}` | Configuration for MDC (Mapped Diagnostic Context). Values specified here will be added to the log context of all incoming requests and can be used in log patterns. |
| logging.requestIdHeaderName | string | `"X-Request-ID"` | The header name to use for the request ID. |
| managementService | object | `{"annotations":{},"clusterIP":"None","externalTrafficPolicy":null,"internalTrafficPolicy":null,"ports":[{"name":"polaris-mgmt","nodePort":null,"port":8182,"protocol":null,"targetPort":null}],"sessionAffinity":null,"trafficDistribution":null,"type":"ClusterIP"}` | Management service settings. These settings are used to configure liveness and readiness probes, and to configure the dedicated headless service that will expose health checks and metrics, e.g. for metrics scraping and service monitoring. |
| managementService.annotations | object | `{}` | Annotations to add to the service. |
| managementService.clusterIP | string | `"None"` | By default, the management service is headless, i.e. it does not have a cluster IP. This is generally the right option for exposing health checks and metrics, e.g. for metrics scraping and service monitoring. |
| managementService.ports | list | `[{"name":"polaris-mgmt","nodePort":null,"port":8182,"protocol":null,"targetPort":null}]` | The ports the management service will listen on. At least one port is required; the first port implicitly becomes the HTTP port that the application will use for serving management requests. By default, it's 8182. Note: port names must be unique and no more than 15 characters long. |
| managementService.ports[0] | object | `{"name":"polaris-mgmt","nodePort":null,"port":8182,"protocol":null,"targetPort":null}` | The name of the management port. Required. |
| managementService.ports[0].nodePort | string | `nil` | The port on each node on which this service is exposed when type is NodePort or LoadBalancer. Usually assigned by the system. If not specified, a port will be allocated if this Service requires one. If this field is specified when creating a Service which does not need it, creation will fail. |
| managementService.ports[0].port | int | `8182` | The port the management service listens on. By default, the management interface is exposed on HTTP port 8182. |
| managementService.ports[0].protocol | string | `nil` | The IP protocol for this port. Supports "TCP", "UDP", and "SCTP". Default is TCP. |
| managementService.ports[0].targetPort | string | `nil` | Number or name of the port to access on the pods targeted by the service. If this is a string, it will be looked up as a named port in the target Pod's container ports. If this is not specified, the value of the 'port' field is used. |
| managementService.type | string | `"ClusterIP"` | The type of service to create. Valid values are: ExternalName, ClusterIP, NodePort, and LoadBalancer. The default value is ClusterIP. |
| metrics.enabled | bool | `true` | Specifies whether metrics for the polaris server should be enabled. |
| metrics.tags | object | `{}` | Additional tags (dimensional labels) to add to the metrics. |
| nodeSelector | object | `{}` | Node labels which must match for the polaris pod to be scheduled on that node. See https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodeselector. |
| oidc | object | `{"authServeUrl":null,"client":{"id":"polaris","secret":{"key":"clientSecret","name":null}},"principalMapper":{"idClaimPath":null,"nameClaimPath":null,"type":"default"},"principalRolesMapper":{"filter":null,"mappings":[],"rolesClaimPath":null,"type":"default"}}` | Polaris OIDC configuration. Only relevant when at least one realm is configured for external (or mixed) authentication. The currently supported configuration is for a single, default OIDC tenant. For more complex scenarios, including OIDC multi-tenancy, you will need to provide the relevant configuration using the `advancedConfig` section. |
| oidc.authServeUrl | string | `nil` | The authentication server URL. Must be provided if at least one realm is configured for external authentication. |
| oidc.client | object | `{"id":"polaris","secret":{"key":"clientSecret","name":null}}` | The client to use when authenticating with the authentication server. |
| oidc.client.id | string | `"polaris"` | The client ID to use when contacting the authentication server's introspection endpoint in order to validate tokens. |
| oidc.client.secret | object | `{"key":"clientSecret","name":null}` | The secret to pull the client secret from. If no client secret is required, leave the secret name unset. |
| oidc.client.secret.key | string | `"clientSecret"` | The key name inside the secret to pull the client secret from. |
| oidc.client.secret.name | string | `nil` | The name of the secret to pull the client secret from. If not provided, the client is assumed to not require a client secret when contacting the introspection endpoint. |
| oidc.principalMapper | object | `{"idClaimPath":null,"nameClaimPath":null,"type":"default"}` | Principal mapping configuration. |
| oidc.principalMapper.idClaimPath | string | `nil` | The path to the claim that contains the principal ID. Nested paths can be expressed using "/" as a separator, e.g. "polaris/principal_id" would look for the "principal_id" field inside the "polaris" object in the token claims. Optional. Either this option or `nameClaimPath` (or both) must be provided. |
| oidc.principalMapper.nameClaimPath | string | `nil` | The claim that contains the principal name. Nested paths can be expressed using "/" as a separator, e.g. "polaris/principal_name" would look for the "principal_name" field inside the "polaris" object in the token claims. Optional. Either this option or `idClaimPath` (or both) must be provided. |
| oidc.principalMapper.type | string | `"default"` | The `PrincipalMapper` implementation to use. Only one built-in type is supported: default. |
| oidc.principalRolesMapper | object | `{"filter":null,"mappings":[],"rolesClaimPath":null,"type":"default"}` | Principal roles mapping configuration. |
| oidc.principalRolesMapper.filter | string | `nil` | A regular expression that matches the role names in the identity. Only roles that match this regex will be included in the Polaris-specific roles. |
| oidc.principalRolesMapper.mappings | list | `[]` | A list of regex mappings that will be applied to each role name in the identity. This can be used to transform the role names in the identity into role names as expected by Polaris. The default Authenticator expects the security identity to expose role names in the format `POLARIS_ROLE:<role name>`. |
| oidc.principalRolesMapper.rolesClaimPath | string | `nil` | The path to the claim that contains the principal roles. Nested paths can be expressed using "/" as a separator, e.g. "polaris/principal_roles" would look for the "principal_roles" field inside the "polaris" object in the token claims. If not set, Quarkus looks for roles in standard locations. See https://quarkus.io/guides/security-oidc-bearer-token-authentication#token-claims-and-security-identity-roles. |
| oidc.principalRolesMapper.type | string | `"default"` | The `PrincipalRolesMapper` implementation to use. Only one built-in type is supported: default. |
| persistence | object | `{"relationalJdbc":{"secret":{"jdbcUrl":"jdbcUrl","name":null,"password":"password","username":"username"}},"type":"in-memory"}` | Polaris persistence configuration. |
| persistence.relationalJdbc | object | `{"secret":{"jdbcUrl":"jdbcUrl","name":null,"password":"password","username":"username"}}` | The configuration for the relational-jdbc persistence manager. |
| persistence.relationalJdbc.secret | object | `{"jdbcUrl":"jdbcUrl","name":null,"password":"password","username":"username"}` | The secret name to pull the database connection properties from. |
| persistence.relationalJdbc.secret.jdbcUrl | string | `"jdbcUrl"` | The secret key holding the database JDBC connection URL |
| persistence.relationalJdbc.secret.name | string | `nil` | The secret name to pull database connection properties from |
| persistence.relationalJdbc.secret.password | string | `"password"` | The secret key holding the database password for authentication |
| persistence.relationalJdbc.secret.username | string | `"username"` | The secret key holding the database username for authentication |
| persistence.type | string | `"in-memory"` | The type of persistence to use. Two built-in types are supported: in-memory and relational-jdbc. |
| podAnnotations | object | `{}` | Annotations to apply to polaris pods. |
| podDisruptionBudget | object | `{"annotations":{},"enabled":false,"maxUnavailable":null,"minAvailable":null}` | Pod disruption budget settings. |
| podDisruptionBudget.annotations | object | `{}` | Annotations to add to the pod disruption budget. |
| podDisruptionBudget.enabled | bool | `false` | Specifies whether a pod disruption budget should be created. |
| podDisruptionBudget.maxUnavailable | string | `nil` | The maximum number of pods that can be unavailable during disruptions. Can be an absolute number (ex: 5) or a percentage of desired pods (ex: 50%). IMPORTANT: Cannot be used simultaneously with minAvailable. |
| podDisruptionBudget.minAvailable | string | `nil` | The minimum number of pods that should remain available during disruptions. Can be an absolute number (ex: 5) or a percentage of desired pods (ex: 50%). IMPORTANT: Cannot be used simultaneously with maxUnavailable. |
| podLabels | object | `{}` | Additional Labels to apply to polaris pods. |
| podSecurityContext | object | `{"fsGroup":10001,"seccompProfile":{"type":"RuntimeDefault"}}` | Security context for the polaris pod. See https://kubernetes.io/docs/tasks/configure-pod-container/security-context/. |
| podSecurityContext.fsGroup | int | `10001` | GID 10001 is compatible with Polaris OSS default images; change this if you are using a different image. |
| rateLimiter | object | `{"tokenBucket":{"requestsPerSecond":9999,"type":"default","window":"PT10S"},"type":"no-op"}` | Polaris rate limiter configuration. |
| rateLimiter.tokenBucket | object | `{"requestsPerSecond":9999,"type":"default","window":"PT10S"}` | The configuration for the default rate limiter, which uses the token bucket algorithm with one bucket per realm. |
| rateLimiter.tokenBucket.requestsPerSecond | int | `9999` | The maximum number of requests per second allowed for each realm. |
| rateLimiter.tokenBucket.type | string | `"default"` | The type of the token bucket rate limiter. Only the default type is supported out of the box. |
| rateLimiter.tokenBucket.window | string | `"PT10S"` | The time window. |
| rateLimiter.type | string | `"no-op"` | The type of rate limiter filter to use. Two built-in types are supported: default and no-op. |
| readinessProbe | object | `{"failureThreshold":3,"initialDelaySeconds":5,"periodSeconds":10,"successThreshold":1,"timeoutSeconds":10}` | Configures the readiness probe for polaris pods. |
| readinessProbe.failureThreshold | int | `3` | Minimum consecutive failures for the probe to be considered failed after having succeeded. Minimum value is 1. |
| readinessProbe.initialDelaySeconds | int | `5` | Number of seconds after the container has started before readiness probes are initiated. Minimum value is 0. |
| readinessProbe.periodSeconds | int | `10` | How often (in seconds) to perform the probe. Minimum value is 1. |
| readinessProbe.successThreshold | int | `1` | Minimum consecutive successes for the probe to be considered successful after having failed. Minimum value is 1. |
| readinessProbe.timeoutSeconds | int | `10` | Number of seconds after which the probe times out. Minimum value is 1. |
| realmContext | object | `{"realms":["POLARIS"],"type":"default"}` | Realm context resolver configuration. |
| realmContext.realms | list | `["POLARIS"]` | List of valid realms, for use with the default realm context resolver. The first realm in the list is the default realm. Realms not in this list will be rejected. |
| realmContext.type | string | `"default"` | The type of realm context resolver to use. Two built-in types are supported: default and test; test is not recommended for production as it does not perform any realm validation. |
| replicaCount | int | `1` | The number of replicas to deploy (horizontal scaling). Beware that replicas are stateless; don't set this number > 1 when using in-memory meta store manager. |
| resources | object | `{}` | Configures the resources requests and limits for polaris pods. We usually recommend not to specify default resources and to leave this as a conscious choice for the user. This also increases chances charts run on environments with little resources, such as Minikube. If you do want to specify resources, uncomment the following lines, adjust them as necessary, and remove the curly braces after 'resources:'. |
| revisionHistoryLimit | string | `nil` | The number of old ReplicaSets to retain to allow rollback (if not set, the default Kubernetes value is set to 10). |
| service | object | `{"annotations":{},"clusterIP":null,"externalTrafficPolicy":null,"internalTrafficPolicy":null,"ports":[{"name":"polaris-http","nodePort":null,"port":8181,"protocol":null,"targetPort":null}],"sessionAffinity":null,"trafficDistribution":null,"type":"ClusterIP"}` | Polaris main service settings. |
| service.annotations | object | `{}` | Annotations to add to the service. |
| service.clusterIP | string | `nil` | You can specify your own cluster IP address If you define a Service that has the .spec.clusterIP set to "None" then Kubernetes does not assign an IP address. Instead, DNS records for the service will return the IP addresses of each pod targeted by the server. This is called a headless service. See https://kubernetes.io/docs/concepts/services-networking/service/#headless-services |
| service.externalTrafficPolicy | string | `nil` | Controls how traffic from external sources is routed. Valid values are Cluster and Local. The default value is Cluster. Set the field to Cluster to route traffic to all ready endpoints. Set the field to Local to only route to ready node-local endpoints. If the traffic policy is Local and there are no node-local endpoints, traffic is dropped by kube-proxy. |
| service.internalTrafficPolicy | string | `nil` | Controls how traffic from internal sources is routed. Valid values are Cluster and Local. The default value is Cluster. Set the field to Cluster to route traffic to all ready endpoints. Set the field to Local to only route to ready node-local endpoints. If the traffic policy is Local and there are no node-local endpoints, traffic is dropped by kube-proxy. |
| service.ports | list | `[{"name":"polaris-http","nodePort":null,"port":8181,"protocol":null,"targetPort":null}]` | The ports the service will listen on. At least one port is required; the first port implicitly becomes the HTTP port that the application will use for serving API requests. By default, it's 8181. Note: port names must be unique and no more than 15 characters long. |
| service.ports[0] | object | `{"name":"polaris-http","nodePort":null,"port":8181,"protocol":null,"targetPort":null}` | The name of the port. Required. |
| service.ports[0].nodePort | string | `nil` | The port on each node on which this service is exposed when type is NodePort or LoadBalancer. Usually assigned by the system. If not specified, a port will be allocated if this Service requires one. If this field is specified when creating a Service which does not need it, creation will fail. |
| service.ports[0].port | int | `8181` | The port the service listens on. By default, the HTTP port is 8181. |
| service.ports[0].protocol | string | `nil` | The IP protocol for this port. Supports "TCP", "UDP", and "SCTP". Default is TCP. |
| service.ports[0].targetPort | string | `nil` | Number or name of the port to access on the pods targeted by the service. If this is a string, it will be looked up as a named port in the target Pod's container ports. If this is not specified, the value of the 'port' field is used. |
| service.sessionAffinity | string | `nil` | The session affinity for the service. Valid values are: None, ClientIP. The default value is None. ClientIP enables sticky sessions based on the client's IP address. This is generally beneficial to Polaris deployments, but some testing may be required in order to make sure that the load is distributed evenly among the pods. Also, this setting affects only internal clients, not external ones. If Ingress is enabled, it is recommended to set sessionAffinity to None. |
| service.trafficDistribution | string | `nil` | The traffic distribution field provides another way to influence traffic routing within a Kubernetes Service. While traffic policies focus on strict semantic guarantees, traffic distribution allows you to express preferences such as routing to topologically closer endpoints. The only valid value is: PreferClose. The default value is implementation-specific. |
| service.type | string | `"ClusterIP"` | The type of service to create. Valid values are: ExternalName, ClusterIP, NodePort, and LoadBalancer. The default value is ClusterIP. |
| serviceAccount.annotations | object | `{}` | Annotations to add to the service account. |
| serviceAccount.create | bool | `true` | Specifies whether a service account should be created. |
| serviceAccount.name | string | `""` | The name of the service account to use. If not set and create is true, a name is generated using the fullname template. |
| serviceMonitor.enabled | bool | `true` | Specifies whether a ServiceMonitor for Prometheus operator should be created. |
| serviceMonitor.interval | string | `""` | The scrape interval; leave empty to let Prometheus decide. Must be a valid duration, e.g. 1d, 1h30m, 5m, 10s. |
| serviceMonitor.labels | object | `{}` | Labels for the created ServiceMonitor so that Prometheus operator can properly pick it up. |
| serviceMonitor.metricRelabelings | list | `[]` | Relabeling rules to apply to metrics. Ref https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config. |
| storage | object | `{"secret":{"awsAccessKeyId":null,"awsSecretAccessKey":null,"gcpToken":null,"gcpTokenLifespan":null,"name":null}}` | Storage credentials for the server. If the following properties are unset, default credentials will be used, in which case the pod must have the necessary permissions to access the storage. |
| storage.secret | object | `{"awsAccessKeyId":null,"awsSecretAccessKey":null,"gcpToken":null,"gcpTokenLifespan":null,"name":null}` | The secret to pull storage credentials from. |
| storage.secret.awsAccessKeyId | string | `nil` | The key in the secret to pull the AWS access key ID from. Only required when using AWS. |
| storage.secret.awsSecretAccessKey | string | `nil` | The key in the secret to pull the AWS secret access key from. Only required when using AWS. |
| storage.secret.gcpToken | string | `nil` | The key in the secret to pull the GCP token from. Only required when using GCP. |
| storage.secret.gcpTokenLifespan | string | `nil` | The key in the secret to pull the GCP token expiration time from. Only required when using GCP. Must be a valid ISO 8601 duration. The default is PT1H (1 hour). |
| storage.secret.name | string | `nil` | The name of the secret to pull storage credentials from. |
| tasks | object | `{"maxConcurrentTasks":null,"maxQueuedTasks":null}` | Polaris asynchronous task executor configuration. |
| tasks.maxConcurrentTasks | string | `nil` | The maximum number of concurrent tasks that can be executed at the same time. The default is the number of available cores. |
| tasks.maxQueuedTasks | string | `nil` | The maximum number of tasks that can be queued up for execution. The default is Integer.MAX_VALUE. |
| tolerations | list | `[]` | A list of tolerations to apply to polaris pods. See https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/. |
| tracing.attributes | object | `{}` | Resource attributes to identify the polaris service among other tracing sources. See https://opentelemetry.io/docs/reference/specification/resource/semantic_conventions/#service. If left empty, traces will be attached to a service named "Apache Polaris"; to change this, provide a service.name attribute here. |
| tracing.enabled | bool | `false` | Specifies whether tracing for the polaris server should be enabled. |
| tracing.endpoint | string | `"http://otlp-collector:4317"` | The collector endpoint URL to connect to (required). The endpoint URL must have either the http:// or the https:// scheme. The collector must talk the OpenTelemetry protocol (OTLP) and the port must be its gRPC port (by default 4317). See https://quarkus.io/guides/opentelemetry for more information. |
| tracing.sample | string | `"1.0d"` | Which requests should be sampled. Valid values are: "all", "none", or a ratio between 0.0 and "1.0d" (inclusive). E.g. "0.5d" means that 50% of the requests will be sampled. Note: avoid entering numbers here, always prefer a string representation of the ratio. |

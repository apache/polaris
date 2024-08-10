<!---
This README.md file was generated with:
https://github.com/norwoodj/helm-docs
Do not modify the README.md file directly, please modify README.md.gotmpl instead.
To re-generate the README.md file, install helm-docs then run from the repo root:
helm-docs --chart-search-root=helm
-->

# Nessie Helm chart

![Version: 0.1.0](https://img.shields.io/badge/Version-0.1.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square)

A Helm chart for Polaris.

**Homepage:** <https://polaris.io/>

## Maintainers

## Source Code

* <https://github.com/polaris-catalog/polaris>

## Documentation

TODO

## Installation

### From Helm repo
```bash
$ #TODO
```

### From local directory (for development purposes)

From Polaris repo root:

```bash
$ helm install --namespace polaris polaris helm/polaris
```

### Uninstalling the chart

```bash
$ helm uninstall --namespace polaris polaris
```

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` | Affinity and anti-affinity for nessie pods. See https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity. |
| autoscaling.enabled | bool | `false` | Specifies whether automatic horizontal scaling should be enabled. Do not enable this when using ROCKSDB version store type. |
| autoscaling.maxReplicas | int | `3` | The maximum number of replicas to maintain. |
| autoscaling.minReplicas | int | `1` | The minimum number of replicas to maintain. |
| autoscaling.targetCPUUtilizationPercentage | int | `80` | Optional; set to zero or empty to disable. |
| autoscaling.targetMemoryUtilizationPercentage | string | `nil` | Optional; set to zero or empty to disable. |
| configMapLabels | object | `{}` | Additional Labels to apply to nessie configmap. |
| image.configDir | string | `"/app/config"` | The path to the directory where the polaris-server.yml file should be mounted. |
| image.pullPolicy | string | `"IfNotPresent"` | The image pull policy. |
| image.repository | string | `"localhost:5001/polaris"` | The image repository to pull from. |
| image.tag | string | `""` | Overrides the image tag whose default is the chart version. |
| imagePullSecrets | list | `[]` | References to secrets in the same namespace to use for pulling any of the images used by this chart. Each entry is a LocalObjectReference to an existing secret in the namespace. The secret must contain a .dockerconfigjson key with a base64-encoded Docker configuration file. See https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/ for more information. |
| ingress.annotations | object | `{}` | Annotations to add to the ingress. |
| ingress.className | string | `""` | Specifies the ingressClassName; leave empty if you don't want to customize it |
| ingress.enabled | bool | `false` | Specifies whether an ingress should be created. |
| ingress.hosts | list | `[{"host":"chart-example.local","paths":[]}]` | A list of host paths used to configure the ingress. |
| ingress.tls | list | `[]` | A list of TLS certificates; each entry has a list of hosts in the certificate, along with the secret name used to terminate TLS traffic on port 443. |
| nodeSelector | object | `{}` | Node labels which must match for the nessie pod to be scheduled on that node. See https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodeselector. |
| podAnnotations | object | `{}` | Annotations to apply to polaris pods. |
| podLabels | object | `{}` | Additional Labels to apply to nessie pods. |
| podSecurityContext | object | `{}` | Security context for the polaris pod. See https://kubernetes.io/docs/tasks/configure-pod-container/security-context/. |
| polaris_config.authenticator.class | string | `"io.polaris.service.auth.TestInlineBearerTokenPolarisAuthenticator"` |  |
| polaris_config.baseCatalogType | string | `"polaris"` |  |
| polaris_config.callContextResolver.type | string | `"default"` |  |
| polaris_config.cors.allowed-credentials | bool | `true` |  |
| polaris_config.cors.allowed-headers[0] | string | `"*"` |  |
| polaris_config.cors.allowed-methods[0] | string | `"PATCH"` |  |
| polaris_config.cors.allowed-methods[1] | string | `"POST"` |  |
| polaris_config.cors.allowed-methods[2] | string | `"DELETE"` |  |
| polaris_config.cors.allowed-methods[3] | string | `"GET"` |  |
| polaris_config.cors.allowed-methods[4] | string | `"PUT"` |  |
| polaris_config.cors.allowed-origins[0] | string | `"http://localhost:8080"` |  |
| polaris_config.cors.allowed-timing-origins[0] | string | `"http://localhost:8080"` |  |
| polaris_config.cors.exposed-headers[0] | string | `"*"` |  |
| polaris_config.cors.preflight-max-age | int | `600` |  |
| polaris_config.defaultRealms[0] | string | `"default-realm"` |  |
| polaris_config.featureConfiguration.DISABLE_TOKEN_GENERATION_FOR_USER_PRINCIPALS | bool | `true` |  |
| polaris_config.featureConfiguration.ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING | bool | `false` |  |
| polaris_config.featureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES[0] | string | `"S3"` |  |
| polaris_config.featureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES[1] | string | `"GCS"` |  |
| polaris_config.featureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES[2] | string | `"AZURE"` |  |
| polaris_config.featureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES[3] | string | `"FILE"` |  |
| polaris_config.logging.appenders[0].logFormat | string | `"%-5p [%d{ISO8601} - %-6r] [%t] [%X{aid}%X{sid}%X{tid}%X{wid}%X{oid}%X{srv}%X{job}%X{rid}] %c{30}: %m %kvp%n%ex"` |  |
| polaris_config.logging.appenders[0].threshold | string | `"ALL"` |  |
| polaris_config.logging.appenders[0].type | string | `"console"` |  |
| polaris_config.logging.appenders[1].archivedFileCount | int | `14` |  |
| polaris_config.logging.appenders[1].archivedLogFilenamePattern | string | `"./logs/polaris-%d.log.gz"` |  |
| polaris_config.logging.appenders[1].currentLogFilename | string | `"./logs/polaris.log"` |  |
| polaris_config.logging.appenders[1].layout.flattenKeyValues | bool | `false` |  |
| polaris_config.logging.appenders[1].layout.includeKeyValues | bool | `true` |  |
| polaris_config.logging.appenders[1].layout.type | string | `"polaris"` |  |
| polaris_config.logging.appenders[1].threshold | string | `"ALL"` |  |
| polaris_config.logging.appenders[1].type | string | `"file"` |  |
| polaris_config.logging.level | string | `"INFO"` |  |
| polaris_config.logging.loggers."io.polaris" | string | `"DEBUG"` |  |
| polaris_config.logging.loggers."org.apache.iceberg.rest" | string | `"DEBUG"` |  |
| polaris_config.metaStoreManager.type | string | `"in-memory"` |  |
| polaris_config.oauth2.type | string | `"test"` |  |
| polaris_config.realmContextResolver.type | string | `"default"` |  |
| polaris_config.server.adminConnectors[0].port | int | `8182` |  |
| polaris_config.server.adminConnectors[0].type | string | `"http"` |  |
| polaris_config.server.applicationConnectors[0].port | int | `8181` |  |
| polaris_config.server.applicationConnectors[0].type | string | `"http"` |  |
| polaris_config.server.maxThreads | int | `200` |  |
| polaris_config.server.minThreads | int | `10` |  |
| polaris_config.server.requestLog.appenders[0].type | string | `"console"` |  |
| polaris_config.server.requestLog.appenders[1].archive | bool | `true` |  |
| polaris_config.server.requestLog.appenders[1].archivedFileCount | int | `14` |  |
| polaris_config.server.requestLog.appenders[1].archivedLogFilenamePattern | string | `"./logs/requests-%d.log.gz"` |  |
| polaris_config.server.requestLog.appenders[1].currentLogFilename | string | `"./logs/request.log"` |  |
| polaris_config.server.requestLog.appenders[1].type | string | `"file"` |  |
| replicaCount | int | `1` | The number of replicas to deploy (horizontal scaling). Beware that replicas are stateless; don't set this number > 1 when using in-memory meta store manager. |
| resources | object | `{}` | Configures the resources requests and limits for nessie pods. We usually recommend not to specify default resources and to leave this as a conscious choice for the user. This also increases chances charts run on environments with little resources, such as Minikube. If you do want to specify resources, uncomment the following lines, adjust them as necessary, and remove the curly braces after 'resources:'. |
| securityContext | object | `{}` | Security context for the polaris container. See https://kubernetes.io/docs/tasks/configure-pod-container/security-context/. |
| service.annotations | object | `{}` | Annotations to add to the service. |
| service.ports | object | `{"polaris-metrics":8182,"polaris-service":8181}` | The ports the service will listen on. Two ports are required: one for the Polaris service and one for the metrics API. Other ports can be declared as needed. The metrics port is handled differently from other ports as a dedicated headless service is created for it. Note: port names must be unique and no more than 15 characters long. |
| service.sessionAffinity | string | `"None"` | The session affinity for the service. Valid values are: None, ClientIP. ClientIP enables sticky sessions based on the client's IP address. This is generally beneficial to Nessie deployments, but some testing may be required in order to make sure that the load is distributed evenly among the pods. Also, this setting affects only internal clients, not external ones. If Ingress is enabled, it is recommended to set sessionAffinity to None. |
| service.type | string | `"ClusterIP"` | The type of service to create. |
| serviceAccount.annotations | object | `{}` | Annotations to add to the service account. |
| serviceAccount.create | bool | `true` | Specifies whether a service account should be created. |
| serviceAccount.name | string | `""` | The name of the service account to use. If not set and create is true, a name is generated using the fullname template. |
| tolerations | list | `[]` | A list of tolerations to apply to nessie pods. See https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/. |
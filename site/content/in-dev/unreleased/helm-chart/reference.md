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
title: Values Reference
linkTitle: Values Reference
type: docs
weight: 900
---

### Deployment

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| replicaCount | int | `1` | The number of replicas to deploy (horizontal scaling). Beware that replicas are stateless; don't set this number > 1 when using in-memory meta store manager. |
| revisionHistoryLimit | string | `nil` | The number of old ReplicaSets to retain to allow rollback (if not set, the default Kubernetes value is set to 10). |

### Image

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| image.repository | string | `"apache/polaris"` | The image repository to pull from. |
| image.pullPolicy | string | `"IfNotPresent"` | The image pull policy. |
| image.tag | string | `"latest"` | The image tag. |
| image.configDir | string | `"/deployments/config"` | The path to the directory where the application.properties file, and other configuration files, if any, should be mounted. |
| imagePullSecrets | list | `[]` | References to secrets in the same namespace to use for pulling any of the images used by this chart. Each entry is a string referring to an existing secret in the namespace. The secret must contain a .dockerconfigjson key with a base64-encoded Docker configuration file. See https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/ for more information. |

### Service Account

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| serviceAccount.create | bool | `true` | Specifies whether a service account should be created. |
| serviceAccount.annotations | object | `{}` | Annotations to add to the service account. |
| serviceAccount.name | string | `""` | The name of the service account to use. If not set and create is true, a name is generated using the fullname template. |

### Pod Configuration

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| podAnnotations | object | `{}` | Annotations to apply to polaris pods. |
| podLabels | object | `{}` | Additional Labels to apply to polaris pods. |
| configMapLabels | object | `{}` | Additional Labels to apply to polaris configmap. |
| podDisruptionBudget.enabled | bool | `false` | Specifies whether a pod disruption budget should be created. |
| podDisruptionBudget.minAvailable | int | `0` | The minimum number of pods that should remain available during disruptions. Can be an absolute number (ex: 5) or a percentage of desired pods (ex: 50%). IMPORTANT: Cannot be used simultaneously with maxUnavailable. |
| podDisruptionBudget.maxUnavailable | int | `0` | The maximum number of pods that can be unavailable during disruptions. Can be an absolute number (ex: 5) or a percentage of desired pods (ex: 50%). IMPORTANT: Cannot be used simultaneously with minAvailable. |
| podDisruptionBudget.annotations | object | `{}` | Annotations to add to the pod disruption budget. |
| podSecurityContext | object | `{"fsGroup":10001,"seccompProfile":{"type":"RuntimeDefault"}}` | Security context for the polaris pod. See https://kubernetes.io/docs/tasks/configure-pod-container/security-context/. |
| containerSecurityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"runAsNonRoot":true,"runAsUser":10000,"seccompProfile":{"type":"RuntimeDefault"}}` | Security context for the polaris container. See https://kubernetes.io/docs/tasks/configure-pod-container/security-context/. |

### Service

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| service.type | string | `"ClusterIP"` | The type of service to create. Valid values are: ExternalName, ClusterIP, NodePort, and LoadBalancer. The default value is ClusterIP. |
| service.ports[0].name | string | `"polaris-http"` | The name of the port. Required. |
| service.ports[0].port | int | `8181` | The port the service listens on. By default, the HTTP port is 8181. |
| service.ports[0].targetPort | int | `0` | Number of the port to access on the pods targeted by the service. If this is not specified or zero, the value of the 'port' field is used. |
| service.ports[0].nodePort | int | `0` | The port on each node on which this service is exposed when type is NodePort or LoadBalancer. Usually assigned by the system. If not specified or zero, a port will be allocated if this Service requires one. If this field is specified when creating a Service which does not need it, creation will fail. |
| service.ports[0].protocol | string | `"TCP"` | The IP protocol for this port. Supports "TCP", "UDP", and "SCTP". Default is TCP. |
| service.sessionAffinity | string | `"None"` | The session affinity for the service. Valid values are: None, ClientIP. The default value is None. ClientIP enables sticky sessions based on the client's IP address. This is generally beneficial to Polaris deployments, but some testing may be required in order to make sure that the load is distributed evenly among the pods. Also, this setting affects only internal clients, not external ones. If Ingress is enabled, it is recommended to set sessionAffinity to None. |
| service.clusterIP | string | `""` | You can specify your own cluster IP address If you define a Service that has the .spec.clusterIP set to "None" then Kubernetes does not assign an IP address. Instead, DNS records for the service will return the IP addresses of each pod targeted by the server. This is called a headless service. See https://kubernetes.io/docs/concepts/services-networking/service/#headless-services |
| service.internalTrafficPolicy | string | `"Cluster"` | Controls how traffic from internal sources is routed. Valid values are Cluster and Local. The default value is Cluster. Set the field to Cluster to route traffic to all ready endpoints. Set the field to Local to only route to ready node-local endpoints. If the traffic policy is Local and there are no node-local endpoints, traffic is dropped by kube-proxy. |
| service.externalTrafficPolicy | string | `"Cluster"` | Controls how traffic from external sources is routed. Valid values are Cluster and Local. The default value is Cluster. Set the field to Cluster to route traffic to all ready endpoints. Set the field to Local to only route to ready node-local endpoints. If the traffic policy is Local and there are no node-local endpoints, traffic is dropped by kube-proxy. |
| service.trafficDistribution | string | `nil` | The traffic distribution field provides another way to influence traffic routing within a Kubernetes Service. While traffic policies focus on strict semantic guarantees, traffic distribution allows you to express preferences such as routing to topologically closer endpoints. The only valid value is: PreferClose. The default value is implementation-specific. |
| service.annotations | object | `{}` | Annotations to add to the service. |

### Management Service

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| managementService.type | string | `"ClusterIP"` | The type of service to create. Valid values are: ExternalName, ClusterIP, NodePort, and LoadBalancer. The default value is ClusterIP. |
| managementService.ports[0].name | string | `"polaris-mgmt"` | The name of the management port. Required. |
| managementService.ports[0].port | int | `8182` | The port the management service listens on. By default, the management interface is exposed on HTTP port 8182. |
| managementService.ports[0].targetPort | int | `0` | Number of the port to access on the pods targeted by the service. If this is not specified or zero, the value of the 'port' field is used. |
| managementService.ports[0].nodePort | int | `0` | The port on each node on which this service is exposed when type is NodePort or LoadBalancer. Usually assigned by the system. If not specified or zero, a port will be allocated if this Service requires one. If this field is specified when creating a Service which does not need it, creation will fail. |
| managementService.ports[0].protocol | string | `"TCP"` | The IP protocol for this port. Supports "TCP", "UDP", and "SCTP". Default is TCP. |
| managementService.clusterIP | string | `"None"` | By default, the management service is headless, i.e. it does not have a cluster IP. This is generally the right option for exposing health checks and metrics, e.g. for metrics scraping and service monitoring. |
| managementService.sessionAffinity | string | `"None"` | The session affinity for the service. |
| managementService.internalTrafficPolicy | string | `"Cluster"` | Controls how traffic from internal sources is routed. |
| managementService.externalTrafficPolicy | string | `"Cluster"` | Controls how traffic from external sources is routed. |
| managementService.trafficDistribution | string | `nil` | The traffic distribution field. |
| managementService.annotations | object | `{}` | Annotations to add to the service. |

### Extra Services

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| extraServices[0].nameSuffix | string | `""` | The suffix to append to the service name. Required. It must be unique. If it does not start with a hyphen, a hyphen will be inserted between the base service name and the suffix. |
| extraServices[0].type | string | `"LoadBalancer"` | The type of service to create. Valid values are: ExternalName, ClusterIP, NodePort, and LoadBalancer. |
| extraServices[0].ports[0].name | string | `"polaris-extra"` | The name of the port. Required. |
| extraServices[0].ports[0].port | int | `8183` | The port the extra service listens on. |
| extraServices[0].ports[0].targetPort | int | `0` | Number of the port to access on the pods targeted by the service. If this is not specified or zero, the value of the 'port' field is used. |
| extraServices[0].ports[0].nodePort | int | `0` | The port on each node on which this extra service is exposed when type is NodePort or LoadBalancer. Usually assigned by the system. If not specified or zero, a port will be allocated if this Service requires one. If this field is specified when creating a Service which does not need it, creation will fail. |
| extraServices[0].ports[0].protocol | string | `"TCP"` | The IP protocol for this port. Supports "TCP", "UDP", and "SCTP". Default is TCP. |
| extraServices[0].clusterIP | string | `""` | The cluster IP for the extra service. |
| extraServices[0].sessionAffinity | string | `"None"` | The session affinity for the extra service. Valid values are: None, ClientIP. The default value is None. |
| extraServices[0].internalTrafficPolicy | string | `"Cluster"` | Controls how traffic from internal sources is routed. |
| extraServices[0].externalTrafficPolicy | string | `"Cluster"` | Controls how traffic from external sources is routed. |
| extraServices[0].trafficDistribution | string | `nil` | The traffic distribution field. |
| extraServices[0].annotations | object | `{}` | Annotations to add to the extra service. |

### Ingress

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| ingress.className | string | `""` | Specifies the ingressClassName; leave empty if you don't want to customize it. |
| ingress.enabled | bool | `false` | Specifies whether an ingress should be created. |
| ingress.annotations | object | `{}` | Annotations to add to the ingress. |
| ingress.hosts[0].host | string | `"chart-example.local"` | The host name. Required. |
| ingress.hosts[0].paths[0].path | string | `"/"` | The path to match. |
| ingress.hosts[0].paths[0].pathType | string | `"Prefix"` | The type of path. Valid values are: Exact, Prefix, and ImplementationSpecific. |
| ingress.tls[0].secretName | string | `""` | The name of the TLS secret to use to terminate TLS traffic on port 443. Required. |
| ingress.tls[0].hosts | list | `["chart-example1.local","chart-example2.local"]` | A list of hosts in the certificate. |

### Gateway

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| gateway.enabled | bool | `false` | Specifies whether a Gateway should be created. |
| gateway.annotations | object | `{}` | Annotations to add to the Gateway. |
| gateway.className | string | `""` | The name of the GatewayClass to use. |
| gateway.listeners[0].name | string | `"http"` | The name of the listener. Required. |
| gateway.listeners[0].protocol | string | `"HTTP"` | Protocol specifies the network protocol this listener expects to receive. |
| gateway.listeners[0].port | int | `80` | The port number to use for the listener. |
| gateway.listeners[0].hostname | string | `""` | Hostname specifies the virtual hostname to match for protocol types that define this concept. When unspecified, all hostnames are matched. |
| gateway.listeners[0].allowedRoutes | object | `{}` | AllowedRoutes defines the types of routes that MAY be attached to a Listener and the trusted namespaces where those Route resources MAY be present. |
| gateway.addresses | list | `[]` | Optional addresses to request for the Gateway. |

### HTTPRoute

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| httproute.enabled | bool | `false` | Specifies whether an HTTPRoute should be created. |
| httproute.annotations | object | `{}` | Annotations to add to the HTTPRoute. |
| httproute.gatewayName | string | `""` | Name of the Gateway resource to attach to. Required. |
| httproute.gatewayNamespace | string | `"default"` | Namespace where the Gateway is deployed. Required. |
| httproute.sectionName | string | `""` | Section name within the gateway to use (optional). |
| httproute.hosts | list | `["chart-example.local"]` | A list of hostnames that the HTTPRoute should match. |

### Resources and Autoscaling

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| resources | object | `{}` | Configures the resources requests and limits for polaris pods. We usually recommend not to specify default resources and to leave this as a conscious choice for the user. This also increases chances charts run on environments with little resources, such as Minikube. If you do want to specify resources, uncomment the following lines, adjust them as necessary, and remove the curly braces after 'resources:'. |
| autoscaling.enabled | bool | `false` | Specifies whether automatic horizontal scaling should be enabled. Do not enable this when using in-memory version store type. |
| autoscaling.minReplicas | int | `1` | The minimum number of replicas to maintain. |
| autoscaling.maxReplicas | int | `3` | The maximum number of replicas to maintain. |
| autoscaling.targetCPUUtilizationPercentage | int | `80` | Optional; set to zero or empty to disable. |
| autoscaling.targetMemoryUtilizationPercentage | int | `0` | Optional; set to zero or empty to disable. |

### Scheduling

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| priorityClassName | string | `""` | Priority class name for polaris pods. See https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/#pod-priority |
| nodeSelector | object | `{}` | Node labels which must match for the polaris pod to be scheduled on that node. See https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodeselector. |
| tolerations | list | `[]` | A list of tolerations to apply to polaris pods. See https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/. |
| affinity | object | `{}` | Affinity and anti-affinity for polaris pods. See https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity. |
| topologySpreadConstraints | list | `[]` | Topology spread constraints for polaris pods. See https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/#topologyspreadconstraints-field. |

### Probes

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| livenessProbe.initialDelaySeconds | int | `5` | Number of seconds after the container has started before liveness probes are initiated. Minimum value is 0. |
| livenessProbe.periodSeconds | int | `10` | How often (in seconds) to perform the probe. Minimum value is 1. |
| livenessProbe.successThreshold | int | `1` | Minimum consecutive successes for the probe to be considered successful after having failed. Minimum value is 1. |
| livenessProbe.failureThreshold | int | `3` | Minimum consecutive failures for the probe to be considered failed after having succeeded. Minimum value is 1. |
| livenessProbe.timeoutSeconds | int | `10` | Number of seconds after which the probe times out. Minimum value is 1. |
| livenessProbe.terminationGracePeriodSeconds | int | `30` | Optional duration in seconds the pod needs to terminate gracefully upon probe failure. Minimum value is 1. |
| readinessProbe.initialDelaySeconds | int | `5` | Number of seconds after the container has started before readiness probes are initiated. Minimum value is 0. |
| readinessProbe.periodSeconds | int | `10` | How often (in seconds) to perform the probe. Minimum value is 1. |
| readinessProbe.successThreshold | int | `1` | Minimum consecutive successes for the probe to be considered successful after having failed. Minimum value is 1. |
| readinessProbe.failureThreshold | int | `3` | Minimum consecutive failures for the probe to be considered failed after having succeeded. Minimum value is 1. |
| readinessProbe.timeoutSeconds | int | `10` | Number of seconds after which the probe times out. Minimum value is 1. |

### Advanced Configuration

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| advancedConfig | object | `{}` | Advanced configuration. You can pass here any valid Polaris or Quarkus configuration property. Any property that is defined here takes precedence over all the other configuration values generated by this chart. Properties can be passed "flattened" or as nested YAML objects (see examples below). Note: values should be strings; avoid using numbers, booleans, or other types. |
| extraEnv | list | `[]` | Advanced configuration via Environment Variables. Extra environment variables to add to the Polaris server container. You can pass here any valid EnvVar object: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.27/#envvar-v1-core This can be useful to get configuration values from Kubernetes secrets or config maps. |
| extraVolumes | list | `[]` | Extra volumes to add to the polaris pod. See https://kubernetes.io/docs/concepts/storage/volumes/. |
| extraVolumeMounts | list | `[]` | Extra volume mounts to add to the polaris container. See https://kubernetes.io/docs/concepts/storage/volumes/. |
| extraInitContainers | list | `[]` | Add additional init containers to the polaris pod(s) See https://kubernetes.io/docs/concepts/workloads/pods/init-containers/. |

### Observability

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| tracing.enabled | bool | `false` | Specifies whether tracing for the polaris server should be enabled. |
| tracing.endpoint | string | `"http://otlp-collector:4317"` | The collector endpoint URL to connect to (required). The endpoint URL must have either the http:// or the https:// scheme. The collector must talk the OpenTelemetry protocol (OTLP) and the port must be its gRPC port (by default 4317). See https://quarkus.io/guides/opentelemetry for more information. |
| tracing.sample | string | `"1.0d"` | Which requests should be sampled. Valid values are: "all", "none", or a ratio between 0.0 and "1.0d" (inclusive). E.g. "0.5d" means that 50% of the requests will be sampled. Note: avoid entering numbers here, always prefer a string representation of the ratio. |
| tracing.attributes | object | `{}` | Resource attributes to identify the polaris service among other tracing sources. See https://opentelemetry.io/docs/reference/specification/resource/semantic_conventions/#service. If left empty, traces will be attached to a service named "Apache Polaris"; to change this, provide a service.name attribute here. |
| metrics.enabled | bool | `true` | Specifies whether metrics for the polaris server should be enabled. |
| metrics.tags | object | `{}` | Additional tags (dimensional labels) to add to the metrics. |
| serviceMonitor.enabled | bool | `true` | Specifies whether a ServiceMonitor for Prometheus operator should be created. |
| serviceMonitor.interval | string | `""` | The scrape interval; leave empty to let Prometheus decide. Must be a valid duration, e.g. 1d, 1h30m, 5m, 10s. |
| serviceMonitor.labels | object | `{}` | Labels for the created ServiceMonitor so that Prometheus operator can properly pick it up. |
| serviceMonitor.metricRelabelings | list | `[]` | Relabeling rules to apply to metrics. Ref https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config. |

### Logging

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| logging.level | string | `"INFO"` | The log level of the root category, which is used as the default log level for all categories. |
| logging.requestIdHeaderName | string | `"X-Request-ID"` | The header name to use for the request ID. |
| logging.console.enabled | bool | `true` | Whether to enable the console appender. |
| logging.console.threshold | string | `"ALL"` | The log level of the console appender. |
| logging.console.json | bool | `false` | Whether to log in JSON format. |
| logging.console.format | string | `"%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%c{3.}] [%X{requestId},%X{realmId}] [%X{traceId},%X{parentId},%X{spanId},%X{sampled}] (%t) %s%e%n"` | The log format to use. Ignored if JSON format is enabled. See https://quarkus.io/guides/logging#logging-format for details. |
| logging.file.enabled | bool | `false` | Whether to enable the file appender. |
| logging.file.threshold | string | `"ALL"` | The log level of the file appender. |
| logging.file.json | bool | `false` | Whether to log in JSON format. |
| logging.file.format | string | `"%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%c{3.}] [%X{requestId},%X{realmId}] [%X{traceId},%X{parentId},%X{spanId},%X{sampled}] (%t) %s%e%n"` | The log format to use. Ignored if JSON format is enabled. See https://quarkus.io/guides/logging#logging-format for details. |
| logging.file.logsDir | string | `"/deployments/logs"` | The local directory where log files are stored. The persistent volume claim will be mounted here. |
| logging.file.fileName | string | `"polaris.log"` | The log file name. |
| logging.file.rotation.maxFileSize | string | `"100Mi"` | The maximum size of the log file before it is rotated. Should be expressed as a Kubernetes quantity. |
| logging.file.rotation.maxBackupIndex | int | `5` | The maximum number of backup files to keep. |
| logging.file.rotation.fileSuffix | string | `nil` | An optional suffix to append to the rotated log files. If present, the rotated log files will be grouped in time buckets, and each bucket will contain at most maxBackupIndex files. The suffix must be in a date-time format that is understood by DateTimeFormatter. If the suffix ends with .gz or .zip, the rotated files will also be compressed using the corresponding algorithm. |
| logging.file.storage.className | string | `"standard"` | The storage class name of the persistent volume claim to create. |
| logging.file.storage.size | string | `"512Gi"` | The size of the persistent volume claim to create. |
| logging.file.storage.selectorLabels | object | `{}` | Labels to add to the persistent volume claim spec selector; a persistent volume with matching labels must exist. Leave empty if using dynamic provisioning. |
| logging.categories | object | `{}` | Configuration for specific log categories. Keys are category names (e.g., org.apache.polaris), values are log levels. |
| logging.mdc | object | `{}` | Configuration for MDC (Mapped Diagnostic Context). Values specified here will be added to the log context of all incoming requests and can be used in log patterns. |

### Realm Context

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| realmContext.type | string | `"default"` | The type of realm context resolver to use. Two built-in types are supported: default and test; test is not recommended for production as it does not perform any realm validation. |
| realmContext.realms | list | `["POLARIS"]` | List of valid realms, for use with the default realm context resolver. The first realm in the list is the default realm. Realms not in this list will be rejected. |

### Features

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| features.realmOverrides | object | `{}` | Features to enable or disable per realm. This field is a map of maps. The realm name is the key, and the value is a map of feature names to values. If a feature is not present in the map, the default value from the 'defaults' field is used. |

### Persistence

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| persistence.type | string | `"in-memory"` | The type of persistence to use. Three built-in types are supported: in-memory, relational-jdbc, and nosql (beta). |
| persistence.relationalJdbc.secret.name | string | `""` | The secret name to pull database connection properties from |
| persistence.relationalJdbc.secret.username | string | `"username"` | The secret key holding the database username for authentication |
| persistence.relationalJdbc.secret.password | string | `"password"` | The secret key holding the database password for authentication |
| persistence.relationalJdbc.secret.jdbcUrl | string | `"jdbcUrl"` | The secret key holding the database JDBC connection URL |
| persistence.nosql.backend | string | `"MongoDb"` | The NoSQL backend to use. Two built-in types are supported: MongoDb and InMemory. Only MongoDb is supported for production use. |
| persistence.nosql.database | string | `"polaris"` | The MongoDB database name to use. |
| persistence.nosql.secret.name | string | `""` | The secret name to pull the MongoDB connection string from. |
| persistence.nosql.secret.connectionString | string | `"connectionString"` | The secret key holding the MongoDB connection string. |

### File IO

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| fileIo.type | string | `"default"` | The type of file IO to use. Two built-in types are supported: default and wasb. The wasb one translates WASB paths to ABFS ones. |

### Storage

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| storage.secret.name | string | `""` | The name of the secret to pull storage credentials from. |
| storage.secret.awsAccessKeyId | string | `""` | The key in the secret to pull the AWS access key ID from. Only required when using AWS. |
| storage.secret.awsSecretAccessKey | string | `""` | The key in the secret to pull the AWS secret access key from. Only required when using AWS. |
| storage.secret.gcpToken | string | `""` | The key in the secret to pull the GCP token from. Only required when using GCP. |
| storage.secret.gcpTokenLifespan | string | `""` | The key in the secret to pull the GCP token expiration time from. Only required when using GCP. Must be a valid ISO 8601 duration. The default is PT1H (1 hour). |

### Authentication

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| authentication.type | string | `"internal"` | The type of authentication to use. Three built-in types are supported: internal, external, and mixed. |
| authentication.authenticator.type | string | `"default"` | The type of authenticator to use. |
| authentication.tokenService.type | string | `"default"` | The token service implementation to use. Two built-in types are supported: default and disabled. Only relevant when using internal (or mixed) authentication. When using external authentication, the token service is always disabled. |
| authentication.tokenBroker.type | string | `"rsa-key-pair"` | The token broker implementation to use. Two built-in types are supported: rsa-key-pair and symmetric-key. Only relevant when using internal (or mixed) authentication. When using external authentication, the token broker is not used. |
| authentication.tokenBroker.maxTokenGeneration | string | `"PT1H"` | Maximum token generation duration (e.g., PT1H for 1 hour). |
| authentication.tokenBroker.secret.name | string | `""` | The name of the secret to pull the keys from. If not provided, a key pair will be generated. This is not recommended for production. |
| authentication.tokenBroker.secret.rsaKeyPair.publicKey | string | `"public.pem"` | Key name inside the secret for the RSA public key. |
| authentication.tokenBroker.secret.rsaKeyPair.privateKey | string | `"private.pem"` | Key name inside the secret for the RSA private key. |
| authentication.tokenBroker.secret.symmetricKey.secretKey | string | `"symmetric.key"` | Key name inside the secret for the symmetric key. |
| authentication.realmOverrides | object | `{}` | Authentication configuration overrides per realm. |

### OIDC

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| oidc.authServeUrl | string | `""` | The authentication server URL. Must be provided if at least one realm is configured for external authentication. |
| oidc.client.id | string | `"polaris"` | The client ID to use when contacting the authentication server's introspection endpoint in order to validate tokens. |
| oidc.client.secret.name | string | `""` | The name of the secret to pull the client secret from. If not provided, the client is assumed to not require a client secret when contacting the introspection endpoint. |
| oidc.client.secret.key | string | `"clientSecret"` | The key name inside the secret to pull the client secret from. |
| oidc.principalMapper.type | string | `"default"` | The `PrincipalMapper` implementation to use. Only one built-in type is supported: default. |
| oidc.principalMapper.idClaimPath | string | `""` | The path to the claim that contains the principal ID. Nested paths can be expressed using "/" as a separator, e.g. "polaris/principal_id" would look for the "principal_id" field inside the "polaris" object in the token claims. Optional. Either this option or `nameClaimPath` (or both) must be provided. |
| oidc.principalMapper.nameClaimPath | string | `""` | The claim that contains the principal name. Nested paths can be expressed using "/" as a separator, e.g. "polaris/principal_name" would look for the "principal_name" field inside the "polaris" object in the token claims. Optional. Either this option or `idClaimPath` (or both) must be provided. |
| oidc.principalRolesMapper.type | string | `"default"` | The `PrincipalRolesMapper` implementation to use. Only one built-in type is supported: default. |
| oidc.principalRolesMapper.rolesClaimPath | string | `""` | The path to the claim that contains the principal roles. Nested paths can be expressed using "/" as a separator, e.g. "polaris/principal_roles" would look for the "principal_roles" field inside the "polaris" object in the token claims. If not set, Quarkus looks for roles in standard locations. See https://quarkus.io/guides/security-oidc-bearer-token-authentication#token-claims-and-security-identity-roles. |
| oidc.principalRolesMapper.filter | string | `""` | A regular expression that matches the role names in the identity. Only roles that match this regex will be included in the Polaris-specific roles. |
| oidc.principalRolesMapper.mappings | list | `[]` | A list of regex mappings that will be applied to each role name in the identity. This can be used to transform the role names in the identity into role names as expected by Polaris. The default Authenticator expects the security identity to expose role names in the format `POLARIS_ROLE:<role name>`. |

### CORS

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| cors.allowedOrigins | list | `[]` | Origins allowed for CORS, e.g. http://polaris.apache.org, http://localhost:8181. In case an entry of the list is surrounded by forward slashes, it is interpreted as a regular expression. |
| cors.allowedMethods | list | `[]` | HTTP methods allowed for CORS, ex: GET, PUT, POST. If this is not set or empty, all requested methods are considered allowed. |
| cors.allowedHeaders | list | `[]` | HTTP headers allowed for CORS, ex: X-Custom, Content-Disposition. If this is not set or empty, all requested headers are considered allowed. |
| cors.exposedHeaders | list | `[]` | HTTP headers exposed to the client, ex: X-Custom, Content-Disposition. The default is an empty list. |
| cors.accessControlMaxAge | string | `""` | The `Access-Control-Max-Age` response header value indicating how long the results of a pre-flight request can be cached. Must be a valid duration. |
| cors.accessControlAllowCredentials | string | `nil` | The `Access-Control-Allow-Credentials` response header. The value of this header will default to `true` if `allowedOrigins` property is set and there is a match with the precise `Origin` header. |

### Rate Limiter

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| rateLimiter.type | string | `"no-op"` | The type of rate limiter filter to use. Two built-in types are supported: default and no-op. |
| rateLimiter.tokenBucket.type | string | `"default"` | The type of the token bucket rate limiter. Only the default type is supported out of the box. |
| rateLimiter.tokenBucket.requestsPerSecond | int | `9999` | The maximum number of requests (permits) per second allowed for each realm. |

### Tasks

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| tasks.maxConcurrentTasks | int | `0` | The maximum number of concurrent tasks that can be executed at the same time. If unspecified or zero, defaults to the number of available cores. |
| tasks.maxQueuedTasks | int | `0` | The maximum number of tasks that can be queued up for execution. If unspecified or zero, defaults to Integer.MAX_VALUE. |

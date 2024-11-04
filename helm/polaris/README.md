<!---
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

<!---
  This README.md file was generated with:
  https://github.com/norwoodj/helm-docs
  Do not modify the README.md file directly, please modify README.md.gotmpl instead.
  To re-generate the README.md file, install helm-docs then run from the repo root:
  helm-docs --chart-search-root=helm
-->

# Polaris Helm chart

![Version: 0.1.0](https://img.shields.io/badge/Version-0.1.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square)

A Helm chart for Polaris.

**Homepage:** <https://polaris.apache.org/>

## Maintainers

## Source Code

* <https://github.com/apache/polaris>

## Installation

### Optional

When using a custom `persistence.xml`, a Kubernetes Secret must be created for `.persistenceConfigSecret`. Below is a sample command:
```bash
kubectl create secret generic polaris-secret -n polaris --from-file=persistence.xml
```

### From local directory (for development purposes)

From Polaris repo root:

```bash
$ helm install polaris helm/polaris --namespace polaris --create-namespace
```

### Uninstalling the chart

```bash
$ helm uninstall --namespace polaris polaris
```

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` | Affinity and anti-affinity for polaris pods. See https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity. |
| autoscaling.enabled | bool | `false` | Specifies whether automatic horizontal scaling should be enabled. Do not enable this when using in-memory version store type. |
| autoscaling.maxReplicas | int | `3` | The maximum number of replicas to maintain. |
| autoscaling.minReplicas | int | `1` | The minimum number of replicas to maintain. |
| autoscaling.targetCPUUtilizationPercentage | int | `80` | Optional; set to zero or empty to disable. |
| autoscaling.targetMemoryUtilizationPercentage | string | `nil` | Optional; set to zero or empty to disable. |
| bootstrapMetastoreManager | bool | `false` | Configures whether to enable the bootstrap metastore manager job |
| configMapLabels | object | `{}` | Additional Labels to apply to polaris configmap. |
| extraEnv | list | `[]` | Advanced configuration via Environment Variables. Extra environment variables to add to the Polaris server container. You can pass here any valid EnvVar object: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.27/#envvar-v1-core This can be useful to get configuration values from Kubernetes secrets or config maps. |
| image.pullPolicy | string | `"IfNotPresent"` | The image pull policy. |
| image.repository | string | `"localhost:5001/polaris"` | The image repository to pull from. |
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
| nodeSelector | object | `{}` | Node labels which must match for the polaris pod to be scheduled on that node. See https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodeselector. |
| persistenceConfigSecret | string | `nil` | The secret name to pull persistence.xml from (ensure the key name is 'persistence.xml') |
| podAnnotations | object | `{}` | Annotations to apply to polaris pods. |
| podLabels | object | `{}` | Additional Labels to apply to polaris pods. |
| podSecurityContext | object | `{}` | Security context for the polaris pod. See https://kubernetes.io/docs/tasks/configure-pod-container/security-context/. |
| polarisServerConfig | object | `{"authenticator":{"class":"org.apache.polaris.service.auth.TestInlineBearerTokenPolarisAuthenticator"},"callContextResolver":{"type":"default"},"cors":{"allowed-credentials":true,"allowed-headers":["*"],"allowed-methods":["PATCH","POST","DELETE","GET","PUT"],"allowed-origins":["http://localhost:8080"],"allowed-timing-origins":["http://localhost:8080"],"exposed-headers":["*"],"preflight-max-age":600},"defaultRealms":["default-realm"],"featureConfiguration":{"ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING":false,"SUPPORTED_CATALOG_STORAGE_TYPES":["S3","GCS","AZURE","FILE"]},"io":{"factoryType":"default"},"logging":{"appenders":[{"logFormat":"%-5p [%d{ISO8601} - %-6r] [%t] [%X{aid}%X{sid}%X{tid}%X{wid}%X{oid}%X{srv}%X{job}%X{rid}] %c{30}: %m %kvp%n%ex","threshold":"ALL","type":"console"}],"level":"INFO","loggers":{"org.apache.iceberg.rest":"DEBUG","org.apache.polaris":"DEBUG"}},"maxRequestBodyBytes":-1,"metaStoreManager":{"type":"in-memory"},"oauth2":{"type":"test"},"rateLimiter":{"type":"no-op"},"realmContextResolver":{"type":"default"},"server":{"adminConnectors":[{"port":8182,"type":"http"}],"applicationConnectors":[{"port":8181,"type":"http"}],"maxThreads":200,"minThreads":10,"requestLog":{"appenders":[{"type":"console"}]}}}` | Configures for polaris-server.yml |
| readinessProbe | object | `{"failureThreshold":3,"initialDelaySeconds":5,"periodSeconds":10,"successThreshold":1,"timeoutSeconds":10}` | Configures the readiness probe for polaris pods. |
| readinessProbe.failureThreshold | int | `3` | Minimum consecutive failures for the probe to be considered failed after having succeeded. Minimum value is 1. |
| readinessProbe.initialDelaySeconds | int | `5` | Number of seconds after the container has started before readiness probes are initiated. Minimum value is 0. |
| readinessProbe.periodSeconds | int | `10` | How often (in seconds) to perform the probe. Minimum value is 1. |
| readinessProbe.successThreshold | int | `1` | Minimum consecutive successes for the probe to be considered successful after having failed. Minimum value is 1. |
| readinessProbe.timeoutSeconds | int | `10` | Number of seconds after which the probe times out. Minimum value is 1. |
| replicaCount | int | `1` | The number of replicas to deploy (horizontal scaling). Beware that replicas are stateless; don't set this number > 1 when using in-memory meta store manager. |
| resources | object | `{}` | Configures the resources requests and limits for polaris pods. We usually recommend not to specify default resources and to leave this as a conscious choice for the user. This also increases chances charts run on environments with little resources, such as Minikube. If you do want to specify resources, uncomment the following lines, adjust them as necessary, and remove the curly braces after 'resources:'. |
| securityContext | object | `{}` | Security context for the polaris container. See https://kubernetes.io/docs/tasks/configure-pod-container/security-context/. |
| service.annotations | object | `{}` | Annotations to add to the service. |
| service.ports | object | `{"polaris-metrics":8182,"polaris-service":8181}` | The ports the service will listen on. Two ports are required: one for the Polaris service and one for the metrics API. Note: port names must be unique and no more than 15 characters long. |
| service.sessionAffinity | string | `"None"` | The session affinity for the service. Valid values are: None, ClientIP. ClientIP enables sticky sessions based on the client's IP address. This is generally beneficial to Polaris deployments, but some testing may be required in order to make sure that the load is distributed evenly among the pods. Also, this setting affects only internal clients, not external ones. If Ingress is enabled, it is recommended to set sessionAffinity to None. |
| service.type | string | `"ClusterIP"` | The type of service to create. |
| serviceAccount.annotations | object | `{}` | Annotations to add to the service account. |
| serviceAccount.create | bool | `true` | Specifies whether a service account should be created. |
| serviceAccount.name | string | `""` | The name of the service account to use. If not set and create is true, a name is generated using the fullname template. |
| tolerations | list | `[]` | A list of tolerations to apply to polaris pods. See https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/. |
| toolsImage.pullPolicy | string | `"IfNotPresent"` | The image pull policy. |
| toolsImage.repository | string | `"registry.access.redhat.com/ubi9/openjdk-21"` | The image repository to pull from (must have jar binary included). |
| toolsImage.tag | string | `"latest"` | The image tag. |
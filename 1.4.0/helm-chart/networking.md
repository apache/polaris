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
title: Services & Networking
linkTitle: Services & Networking
type: docs
weight: 500
---

This guide describes the Kubernetes services created by the Polaris Helm chart and how to configure external access using the Gateway API or Ingress.

{{< alert tip >}}
The [Kubernetes Gateway API](https://gateway-api.sigs.k8s.io/) is the recommended approach for exposing Polaris externally. It provides a more expressive, extensible, and role-oriented API compared to Ingress, and is the future direction for Kubernetes traffic management.
{{< /alert >}}

## Services

The Polaris Helm chart creates the following services by default:

### Main Service

The main service exposes the Polaris REST APIs. By default, it is a `ClusterIP` service listening on port 8181.

| Setting                   | Default        | Description                                            |
|---------------------------|----------------|--------------------------------------------------------|
| `service.type`            | `ClusterIP`    | Service type (`ClusterIP`, `NodePort`, `LoadBalancer`) |
| `service.ports[0].port`   | `8181`         | The port the service listens on                        |
| `service.ports[0].name`   | `polaris-http` | The name of the port                                   |
| `service.sessionAffinity` | `None`         | Session affinity (`None` or `ClientIP`)                |
| `service.clusterIP`       | `""`           | Set to `None` for a headless service                   |

Example configuration:

```yaml
service:
  type: ClusterIP
  ports:
    - name: polaris-http
      port: 8181
      protocol: TCP
```

### Management Service

The management service exposes health checks and metrics endpoints. By default, it is a headless service (`clusterIP: None`) listening on port 8182, which is ideal for Prometheus scraping and service monitoring.

| Setting                           | Default        | Description                                |
|-----------------------------------|----------------|--------------------------------------------|
| `managementService.type`          | `ClusterIP`    | Service type                               |
| `managementService.ports[0].port` | `8182`         | The port the management service listens on |
| `managementService.ports[0].name` | `polaris-mgmt` | The name of the port                       |
| `managementService.clusterIP`     | `None`         | Headless by default                        |

Example configuration:

```yaml
managementService:
  type: ClusterIP
  clusterIP: None  # Headless service for metrics scraping
  ports:
    - name: polaris-mgmt
      port: 8182
      protocol: TCP
```

### Extra Services

You can define additional services using `extraServices`. This is useful when you need to expose the same pods with different service configurations, such as exposing the API via a LoadBalancer in addition to the default ClusterIP service.

```yaml
extraServices:
  - nameSuffix: "lb"
    type: LoadBalancer
    ports:
      - name: polaris-http
        port: 8181
        targetPort: 8181
        protocol: TCP
    annotations:
      service.beta.kubernetes.io/aws-load-balancer-type: "nlb-ip"
```

This creates an additional service named `<release-name>-polaris-lb` of type `LoadBalancer`.

## Gateway API

The [Kubernetes Gateway API](https://gateway-api.sigs.k8s.io/) is a more expressive and extensible alternative to Ingress. The Polaris Helm chart supports creating Gateway and HTTPRoute resources.

{{< alert note >}}
Ingress and HTTPRoute are mutually exclusive. Only one can be enabled at a time.
{{< /alert >}}

{{< alert tip >}}
In most production environments, the Gateway resource is cluster-wide and managed by cluster administrators. In this case, you should only configure the HTTPRoute to attach to an existing Gateway. The option to create a Gateway via this Helm chart is provided as a convenience for small deployments or development environments.
{{< /alert >}}

### Prerequisites

The Gateway API CRDs must be installed in your cluster. In most production environments, this is handled by cluster administrators. To install manually:

```bash
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/latest/download/standard-install.yaml
```

You also need a Gateway controller (e.g., Envoy Gateway, Istio, Contour) installed and a `GatewayClass` defined.

### Gateway Configuration

The chart can create a Gateway resource that defines how traffic enters the cluster:

```yaml
gateway:
  enabled: true
  className: "cluster-gateway"
  listeners:
    - name: http
      protocol: HTTP
      port: 80
    - name: https
      protocol: HTTPS
      port: 443
      hostname: "polaris.example.com"
      tls:
        mode: Terminate
        certificateRefs:
          - name: polaris-tls
            kind: Secret
```

### Gateway with Static IP Address

Request a specific IP address for the Gateway:

```yaml
gateway:
  enabled: true
  className: "eg"
  listeners:
    - name: http
      protocol: HTTP
      port: 80
  addresses:
    - type: IPAddress
      value: 192.168.1.100
```

### HTTPRoute Configuration

HTTPRoute defines how requests are routed from a Gateway to the Polaris service:

```yaml
httproute:
  enabled: true
  gatewayName: "polaris-gateway" # Name of the Gateway to attach to
  gatewayNamespace: "polaris"    # Namespace where the Gateway is deployed
  hosts:
    - polaris.example.com
```

### Using an External Gateway

If you have a shared Gateway managed separately (e.g., by cluster administrators), you can create only the HTTPRoute:

```yaml
gateway:
  enabled: false

httproute:
  enabled: true
  gatewayName: "shared-gateway"
  gatewayNamespace: "gateway-system"
  sectionName: "https"  # Optional: attach to a specific listener
  hosts:
    - polaris.example.com
```

## Ingress

Ingress provides HTTP(S) routing from outside the cluster to services within the cluster. Enable Ingress to expose Polaris externally.

{{< alert note >}}
Ingress and HTTPRoute are mutually exclusive. Only one can be enabled at a time.
{{< /alert >}}

{{< alert warning >}}
The Kubernetes community [Ingress NGINX](https://github.com/kubernetes/ingress-nginx) controller (not to be confused with the [F5 NGINX Ingress Controller](https://github.com/nginx/kubernetes-ingress)) is being retired in March 2026. If you are currently using `className: "nginx"` with `nginx.ingress.kubernetes.io/*` annotations, consider migrating to the Gateway API or an actively maintained ingress controller such as [Traefik](https://traefik.io/), [HAProxy](https://haproxy-ingress.github.io/), [Contour](https://projectcontour.io/), or the [F5 NGINX Ingress Controller](https://docs.nginx.com/nginx-ingress-controller/).
{{< /alert >}}

### Basic Ingress Configuration

```yaml
ingress:
  enabled: true
  className: "traefik"  # Your ingress controller class (e.g., "traefik", "haproxy", "contour")
  hosts:
    - host: polaris.example.com
      paths:
        - path: /
          pathType: Prefix
```

### Ingress with TLS

To enable TLS termination, create a Kubernetes secret containing your TLS certificate and reference it in the Ingress configuration:

```bash
kubectl create secret tls polaris-tls \
  --namespace polaris \
  --cert=path/to/tls.crt \
  --key=path/to/tls.key
```

```yaml
ingress:
  enabled: true
  className: "traefik"  # Your ingress controller class
  hosts:
    - host: polaris.example.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: polaris-tls
      hosts:
        - polaris.example.com
```

## Traffic Policies

Both the main service and management service support traffic distribution policies:

### Internal Traffic Policy

Controls how traffic from within the cluster is routed:

```yaml
service:
  internalTrafficPolicy: Local  # Route only to node-local endpoints
```

### External Traffic Policy

Controls how traffic from outside the cluster is routed (only applicable for `NodePort` and `LoadBalancer` services):

```yaml
service:
  type: LoadBalancer
  externalTrafficPolicy: Local  # Preserve client source IP
```

### Traffic Distribution (Kubernetes 1.31+)

For Kubernetes 1.31 and later, you can use traffic distribution hints:

```yaml
service:
  trafficDistribution: PreferClose  # Prefer topologically closer endpoints
```

## Session Affinity

For better performance, consider enabling session affinity to route requests from the same client to the same pod:

```yaml
service:
  sessionAffinity: ClientIP
```

{{< alert note >}}
Session affinity affects only internal clients. For external traffic through Ingress, configure sticky sessions in your ingress controller.
{{< /alert >}}

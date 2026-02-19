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
title: Production Configuration
linkTitle: Production Configuration
type: docs
weight: 200
---

This guide provides instructions for configuring the Apache Polaris Helm chart for a production environment. For full list of chart values, see the [Chart Reference]({{% relref "reference" %}}) page.

## Prerequisites

- A Kubernetes cluster (1.33+ recommended)
- Helm 3.x or 4.x installed
- `kubectl` configured to access your cluster
- A PostgreSQL or MongoDB database

## Adding the Helm Repository

Add the official Apache Polaris Helm repository:

```bash
helm repo add polaris https://downloads.apache.org/polaris/helm-chart
helm repo update
```

## Installation

Create a `values.yaml` file with your production configuration. See the Chart [Values Reference]({{% relref "reference" %}}) for all available configuration options.

Create the target namespace and install the chart:

```bash
kubectl create namespace polaris
helm install polaris polaris/polaris --namespace polaris --values your-production-values.yaml
```

{{< alert note >}}
For Apache Polaris releases up to 1.3.0-incubating, the `--devel` flag is required for `helm` invocations.
Helm treats the -incubating suffix as a pre‑release by SemVer rules, and will skip charts that are not in a stable versioning scheme by default.
{{< /alert >}}

Verify the installation:

```bash
helm test polaris --namespace polaris
```

## Production Configuration

The default Helm chart values are suitable for development and testing, but they are not recommended for production. The following sections describe the key areas to configure for a production deployment.

### Authentication

Polaris supports internal authentication (with RSA key pairs or symmetric keys) and external authentication via OIDC with identity providers like Keycloak, Okta, Azure AD, and others.

By default, the Polaris Helm chart uses internal authentication with auto-generated keys. In a multi-replica production environment, all Polaris pods must share the same token signing keys to avoid token validation failures.

See the [Authentication]({{% relref "authentication" %}}) page for detailed configuration instructions.

### Persistence

By default, the Polaris Helm chart uses the `in-memory` metastore, which is not suitable for production. A persistent metastore must be configured to ensure data is not lost when pods restart.

Polaris supports PostgreSQL (JDBC) and MongoDB (NoSQL, beta) as production-ready metastores. See the [Persistence]({{% relref "persistence" %}}) page for detailed configuration instructions.

### Networking

For configuring external access to Polaris using the Gateway API or Ingress, see the [Services & Networking]({{% relref "networking" %}}) guide.

### Resource Management

For a production environment, it is crucial to define resource requests and limits for the Polaris pods. Resource requests ensure that pods are allocated enough resources to run, while limits prevent them from consuming too many resources on the node.

Define resource requests and limits for the Polaris pods:

```yaml
resources:
  requests:
    memory: "8Gi"
    cpu: "4"
  limits:
    memory: "8Gi"
    cpu: "4"
```

Adjust these values based on expected workload and available cluster resources.

### Scaling

For high availability, multiple replicas of the Polaris server can be run. This requires a persistent metastore to be configured as described above.

#### Static Replicas

`replicaCount` must be set to the desired number of pods:

```yaml
replicaCount: 3
```

#### Autoscaling

Horizontal autoscaling can be enabled to define the minimum and maximum number of replicas, and CPU or memory utilization targets:

```yaml
autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 5
  targetCPUUtilizationPercentage: 80
  targetMemoryUtilizationPercentage: 80
```

#### Pod Topology Spreading

For better fault tolerance, `topologySpreadConstraints` can be used to distribute pods across different nodes, racks, or availability zones. This helps prevent a single infrastructure failure from taking down all Polaris replicas.

Here is an example that spreads pods across different zones and keeps the number of pods in each zone from differing by more than one:

```yaml
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: "topology.kubernetes.io/zone"
    whenUnsatisfiable: "DoNotSchedule"
```

### Pod Priority

In a production environment, it is advisable to set a `priorityClassName` for the Polaris pods. This ensures that the Kubernetes scheduler gives them higher priority over less critical workloads, and helps prevent them from being evicted from a node that is running out of resources.

First, a `PriorityClass` must be created in the cluster. For example:

```yaml
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: polaris-high-priority
value: 1000000
globalDefault: false
description: "This priority class should be used for Polaris service pods only."
```

Then, the `priorityClassName` can be set in the `values.yaml` file:

```yaml
priorityClassName: "polaris-high-priority"
```

## Bootstrapping Realms

When installing Polaris for the first time, it is necessary to bootstrap each realm using the Polaris admin tool. 

For more information on bootstrapping realms, see the [Admin Tool]({{% relref "../admin-tool#bootstrapping-realms-and-principal-credentials" %}}) guide.

Example for the PostgreSQL metastore:

```bash
kubectl run polaris-bootstrap \
  -n polaris \
  --image=apache/polaris-admin-tool:latest \
  --restart=Never \
  --rm -it \
  --env="polaris.persistence.type=relational-jdbc" \
  --env="quarkus.datasource.username=$(kubectl get secret polaris-persistence -n polaris -o jsonpath='{.data.username}' | base64 --decode)" \
  --env="quarkus.datasource.password=$(kubectl get secret polaris-persistence -n polaris -o jsonpath='{.data.password}' | base64 --decode)" \
  --env="quarkus.datasource.jdbc.url=$(kubectl get secret polaris-persistence -n polaris -o jsonpath='{.data.jdbcUrl}' | base64 --decode)" \
  -- \
  bootstrap -r polaris-realm1 -c polaris-realm1,root,$ROOT_PASSWORD
```

Example for the NoSQL (MongoDB) metastore:

```bash
kubectl run polaris-bootstrap \
  -n polaris \
  --image=apache/polaris-admin-tool:latest \
  --restart=Never \
  --rm -it \
  --env="polaris.persistence.type=nosql" \
  --env="polaris.persistence.nosql.backend=MongoDb" \
  --env="quarkus.mongodb.database=polaris" \
  --env="quarkus.mongodb.connection-string=$(kubectl get secret polaris-nosql-persistence -n polaris -o jsonpath='{.data.connectionString}' | base64 --decode)" \
  -- \
  bootstrap -r polaris-realm1 -c polaris-realm1,root,$ROOT_PASSWORD
```

Both commands above bootstrap a realm named `polaris-realm1` with root password: `$ROOT_PASSWORD`.

{{< alert warning >}}
Replace `$ROOT_PASSWORD` with a strong, unique password for the root credentials.
{{< /alert >}}

## Uninstalling

```bash
helm uninstall --namespace polaris polaris
kubectl delete namespace polaris
```

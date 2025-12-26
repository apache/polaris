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
title: Configuring Helm for Production
linkTitle: Configuring Helm
type: docs
weight: 601
---

This guide provides instructions for configuring the Polaris Helm chart for a production environment. For full list of chart values, see the [main Helm chart documentation](../../helm/).

The default Helm chart values are suitable for development and testing, but they are not recommended for production. Following are the key areas to consider for production deployment.

## Persistence

By default, the Polaris Helm chart uses an `in-memory` metastore, which is not suitable for production. A persistent backend must be configured to ensure data is not lost when pods restart.

To use a persistent backend, `persistence.type` must be set to `relational-jdbc`, and a Kubernetes secret containing the database connection details must be provided.

```yaml
persistence:
  type: relational-jdbc
  relationalJdbc:
    secret:
      name: "polaris-persistence-secret" # A secret containing db credentials
      username: "username"
      password: "password"
      jdbcUrl: "jdbcUrl"
```

## Resource Management

For a production environment, it is crucial to define resource requests and limits for the Polaris pods. Resource requests ensure that pods are allocated enough resources to run, while limits prevent them from consuming too many resources on the node.

Resource requests and limits can be set in the `values.yaml` file:

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

## Pod Priority

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

## Authentication

In a multi-replica production environment, all Polaris pods must share the same token signing keys. The default chart generates random keys for each pod, which will cause token validation failures.

To use a shared set of keys, a Kubernetes secret to store an RSA key pair or a symmetric key must first be created.

### RSA Key Pair

```yaml
authentication:
  tokenBroker:
    type: rsa-key-pair
    secret:
      name: "polaris-rsa-key-pair-secret" # A secret containing the RSA key pair
      rsaKeyPair:
        publicKey: "public.pem"
        privateKey: "private.pem"
```

### Symmetric Key

```yaml
authentication:
  tokenBroker:
    type: symmetric-key
    secret:
      name: "polaris-symmetric-key-secret" # A secret containing the symmetric key
      symmetricKey:
        secretKey: "symmetric.key"
```

## Scaling

For high availability, multiple replicas of the Polaris server can be run. This requires a persistent backend to be configured as described above.

### Static Replicas

`replicaCount` must be set to the desired number of pods.

```yaml
replicaCount: 3
```

### Autoscaling

`autoscaling` can be enabled to define the minimum and maximum number of replicas, and CPU or memory utilization targets.

```yaml
autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 5
  targetCPUUtilizationPercentage: 80
  targetMemoryUtilizationPercentage: 80
```

### Pod Topology Spreading

For better fault tolerance, `topologySpreadConstraints` can be used to distribute pods across different nodes, racks, or availability zones. This helps prevent a single infrastructure failure from taking down all Polaris replicas.

Here is an example that spreads pods across different zones and keeps the number of pods in each zone from differing by more than one:

```yaml
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: "topology.kubernetes.io/zone"
    whenUnsatisfiable: "DoNotSchedule"
```

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
title: Polaris Helm Chart
linkTitle: Helm Chart
type: docs
weight: 650
---

[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/apache-polaris)](https://artifacthub.io/packages/search?repo=apache-polaris)

The Apache Polaris project provides an official Helm chart for deploying Polaris on Kubernetes. The chart supports a wide range of configuration options, including persistence backends, authentication, scaling, observability, and more.

The chart is available in the [official Apache Polaris Helm repository](https://downloads.apache.org/polaris/helm-chart).

## Documentation

- **[Production Installation]({{% relref "production" %}})**: Instructions for deploying Polaris in a production environment, including how to add the Helm repository and set up scaling.

- **[Authentication]({{% relref "authentication" %}})**: Configuration guide for internal authentication (RSA/symmetric keys) and external authentication with OIDC identity providers.

- **[Persistence]({{% relref "persistence" %}})**: Configuration guide for persistence backends, including PostgreSQL (JDBC) and MongoDB (NoSQL).

- **[Services & Networking]({{% relref "networking" %}})**: Configuration guide for Kubernetes services, Ingress, and Gateway API.

- **[Development & Testing]({{% relref "dev" %}})**: Developer-oriented instructions for local development with Minikube, building from source, and running chart tests.

- **[Chart Reference]({{% relref "reference" %}})**: Complete reference of all available Helm chart values and their descriptions.

## Quick Start

To quickly get started with the Polaris Helm chart:

```bash
helm repo add polaris https://downloads.apache.org/polaris/helm-chart
helm repo update
helm upgrade --install polaris polaris/polaris --namespace polaris --create-namespace
```

{{< alert note >}}
For Apache Polaris releases up to 1.3.0-incubating, the `--devel` flag is required for `helm` invocations.
Helm treats the -incubating suffix as a pre‑release by SemVer rules, and will skip charts that are not in a stable versioning scheme by default.
{{< /alert >}}

For production deployments, see the [Production Installation]({{% relref "production" %}}) guide.

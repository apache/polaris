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
title: Development & Testing
linkTitle: Development & Testing
type: docs
weight: 800
---

This guide provides instructions for developers who want to work with the Polaris Helm chart locally, including setting up a local Kubernetes cluster, building from source, and running tests.

## Local Development with Minikube

### Prerequisites

- [Minikube](https://minikube.sigs.k8s.io/docs/start/) installed
- [Helm](https://helm.sh/docs/intro/install/) 3.x or 4.x installed
- [kubectl](https://kubernetes.io/docs/tasks/tools/) installed

### Starting Minikube

Start a local Minikube cluster:

```bash
minikube start
```

### Installing from the Official Repository

Add the official Polaris Helm repository and install the chart:

```bash
helm repo add polaris https://downloads.apache.org/polaris/helm-chart
helm repo update
kubectl create namespace polaris
helm install polaris polaris/polaris --namespace polaris
```

{{< alert note >}}
For Apache Polaris releases up to 1.3.0-incubating, the `--devel` flag is required for `helm` invocations.
Helm treats the -incubating suffix as a pre‑release by SemVer rules, and will skip charts that are not in a stable versioning scheme by default.
{{< /alert >}}

Verify the installation:

```bash
helm test polaris --namespace polaris
```

## Building and Installing from Source

This section assumes you have cloned the Polaris Git repository and set up prerequisites to build the project. See the [Install Dependencies]({{% relref "../getting-started/install-dependencies" %}}) guide for details.

### Building Container Images

Start Minikube and configure Docker to use Minikube's Docker daemon:

```bash
minikube start
eval $(minikube docker-env)
```

Build the container images. The Polaris server image is required, the admin tool image is optional (useful for bootstrapping realms if necessary, see below):

```bash
./gradlew \
  :polaris-server:assemble \
  :polaris-server:quarkusAppPartsBuild --rerun \
  :polaris-admin:assemble \
  :polaris-admin:quarkusAppPartsBuild --rerun \
  -Dquarkus.container-image.build=true
```

Alternatively, you can use Make to start Minikube and build the images:

```bash
make minikube-start-cluster
make build
make minikube-load-images
```

{{< alert tip >}}
Do not use the `minikube-load-images` target on a shell session where you have already evaluated `$(minikube docker-env)`, or the target won't be able to load the images into Minikube.
{{< /alert >}}

### Creating the Namespace and Fixtures

Create the namespace and deploy fixtures:

```bash
kubectl create namespace polaris
kubectl apply --namespace polaris -f helm/polaris/ci/fixtures/
```

The fixtures deploy a PostgreSQL instance and a MongoDB instance that can be used for testing purposes. If you plan to test with persistence backends, wait for the database pods to be ready:

```bash
kubectl wait --namespace polaris --for=condition=ready pod --selector=app.kubernetes.io/name=postgres --timeout=120s
kubectl wait --namespace polaris --for=condition=ready pod --selector=app.kubernetes.io/name=mongodb --timeout=120s
```

Alternatively, you can use Make to deploy the fixtures:

```bash
make helm-fixtures
```

{{< alert warning >}}
The fixtures in `helm/polaris/ci/fixtures/` are intended for testing purposes only and are not suitable for production use. Especially, the PostgreSQL and MongoDB instances are configured without encryption or security and should never be deployed as is in production!
{{< /alert >}}

### Installing the Chart Manually

Install the chart from the local source, using either the `values.yaml` file, or any of the values files in `helm/polaris/ci/`. Example:

```bash
# Non-persistent backend
helm upgrade --install --namespace polaris polaris helm/polaris

# Persistent backend
helm upgrade --install --namespace polaris \
  --values helm/polaris/ci/persistence-values.yaml \
  polaris helm/polaris
```

### Bootstrapping Realms for Development

When doing adhoc testing with a persistent backend, you may want to bootstrap all the realms using the admin tool. For more information, see the [Admin Tool]({{% relref "../admin-tool#bootstrapping-realms-and-principal-credentials" %}}) guide.

Example for the PostgreSQL backend:

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
  bootstrap -r POLARIS -c POLARIS,root,s3cr3t
```

Example for the NoSQL (MongoDB) backend:

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
  bootstrap -r POLARIS -c POLARIS,root,s3cr3t
```

Both commands above bootstrap a realm named `POLARIS` with root password: `s3cr3t`.

{{< alert note >}}
The Helm integration tests (see below) do not require bootstrapping realms, as they do not exercise (yet) Polaris REST APIs.
{{< /alert >}}

## Running Chart Tests

### Prerequisites

Install the required tools:

```bash
brew install chart-testing
brew install yamllint
make helm-install-plugins
```

The following tools will be installed:

* [Helm Unit Test](https://github.com/helm-unittest/helm-unittest)
* [Helm JSON Schema](https://github.com/losisin/helm-values-schema-json)
* [Chart Testing](https://github.com/helm/chart-testing)
* [yamllint](https://github.com/adrienverge/yamllint)

### Unit Tests

Helm unit tests do not require a Kubernetes cluster. Run them from the Polaris repo root:

```bash
make helm-unittest
```

### Linting

Lint the chart using the Chart Testing tool:

```bash
make helm-lint
```

### Making Changes to Documentation or Schema

If you make changes to the Helm chart's `values.yaml` file, you need to regenerate the documentation and schema. Run from the Polaris repo root:

```bash
make helm-schema-generate helm-doc-generate
```

Alternatively, you can run:

```bash
make helm
```

This will run all Helm-related targets, including unit tests, linting, schema generation, and documentation generation (it will not run integration tests though).

### Integration Tests

Integration tests are tests executed with the `ct install` tool. They require a Kubernetes cluster with fixtures deployed, as explained in the [Installing the Chart Manually](#installing-the-chart-manually) section above.

The simplest way to run the integration tests is to use Make:

```bash
make helm-integration-test
```

The above command will build and load the images into Minikube, deploy the fixtures, and run all `ct install` tests.

## Cleanup

Uninstall the chart, remove resources and delete the namespace:

```bash
helm uninstall --namespace polaris polaris
kubectl delete namespace polaris --wait=true --ignore-not-found
```

The stop Minikube if desired:

```bash
minikube stop
```

Alternatively, you can use Make to perform the cleanup:

```bash
make helm-fixtures-cleanup
make minikube-stop-cluster
make minikube-cleanup # will delete the Minikube cluster
```

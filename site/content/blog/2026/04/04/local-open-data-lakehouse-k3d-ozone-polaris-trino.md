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
title: "Build a Local Open Data Lakehouse with k3d, Apache Ozone, Apache Polaris and Trino"
date: 2026-04-04
author: CG Poh
---

> **TL;DR** — Spin up a fully integrated, locally-running open data lakehouse on your laptop
> in under 30 minutes using Kubernetes in Docker (k3d), Apache Ozone as S3-compatible
> object storage, Apache Polaris as the Iceberg REST catalog and Trino as the SQL query
> engine. No cloud account required.

---

## Why This Stack?

The modern open data lakehouse is built on open standards: **Apache Iceberg** as the table
format, a **REST catalog** to manage metadata, object storage for the actual files and a
decoupled compute engine for queries. This separation lets you swap any layer without
rewriting the others.

But spinning up a realistic multi-component stack locally has historically meant juggling
`docker-compose` files, manual wiring and frustrating networking issues. Helm + k3d changes
that. You get a real Kubernetes environment (with proper service discovery, namespaces and
resource management) running entirely inside Docker on your laptop.

Here's what each tool does in our stack:

| Tool | Role | Why |
|---|---|---|
| **k3d** | Local Kubernetes cluster inside Docker | Lightweight, fast to create/destroy, great for dev/test |
| **Apache Ozone** | S3-compatible distributed object store | Stores the actual Iceberg data and metadata files |
| **Apache Polaris** | Iceberg REST catalog (Apache Top-Level Project) | Manages table metadata; any Iceberg-compatible engine can use it |
| **Trino** | Distributed SQL query engine | Reads Iceberg tables via Polaris, files from Ozone |

The data flow looks like this:

```
┌─────────┐  1. catalog ops            ┌────────────────┐
│  Trino  │ ────────────────────────▶  │ Apache Polaris │
│ (Query) │ ◀────────────────────────  │   (Catalog)    │
└────┬────┘  2. metadata location      └───────┬────────┘
     │                                         │ 3. write metadata
     │ 4. read/write data files (S3 API)       │    JSON to Ozone
     ▼                                         ▼
┌────────────────────────────────────────────────────────┐
│                 Apache Ozone (S3 Gateway)              │
│                    (Object Storage)                    │
└────────────────────────────────────────────────────────┘
```

When you run a query:
1. Trino calls Polaris (via the Iceberg REST API) to get table metadata — schema, snapshot, and the location of data files in Ozone
2. Polaris also handles commit orchestration: when Trino creates or writes a table, Polaris writes the Iceberg metadata JSON files directly to Ozone
3. Trino reads and writes the actual Parquet data files **directly to Ozone** using static S3 credentials configured in its Helm values

> **Why does Trino go directly to Ozone instead of through Polaris?**
> In a production cloud setup, Polaris would use AWS STS to vend short-lived, scoped
> credentials to Trino for each table access (credential vending). Trino would then use
> those temporary credentials to hit S3. However, Ozone has no STS endpoint currently, so credential
> vending doesn't work here. Instead, we configure `stsUnavailable: true` on the catalog and
> give Trino static Ozone credentials directly in `trino-values.yaml`. The architecture is
> otherwise identical to a production deployment.

---

## Prerequisites

Before starting, make sure you have the following installed:

- **Docker Engine** (e.g., via Docker Desktop or Colima) — k3d runs Kubernetes inside Docker
- **k3d** ≥ v5.x — `brew install k3d` / [k3d.io](https://k3d.io)
- **kubectl** — `brew install kubectl`
- **Helm** ≥ v3.x — `brew install helm`
- **curl** + **jq** — for Polaris REST API calls
- **AWS CLI** (`aws`) — for verifying Ozone S3 connectivity
  ```bash
  brew install awscli
  ```

**Minimum hardware:** 8 GB RAM and 4 CPU cores recommended. Ozone is resource-hungry.

---

## Step 1 — Create a k3d Cluster

We create a k3d cluster with Traefik (k3d's built-in ingress controller) enabled and map
host port **8080** to the cluster's load balancer port 80. This lets us use clean
host-based routing without touching `/etc/hosts`, thanks to [nip.io](https://nip.io) — a
free public wildcard DNS that resolves anything like `*.127.0.0.1.nip.io` to `127.0.0.1`
automatically, with zero configuration.

> **Offline / no internet?** nip.io requires a DNS lookup. If you're working offline, add
> this to `/etc/hosts` instead and everything will work identically:
> ```bash
> sudo tee -a /etc/hosts <<EOF
> 127.0.0.1 polaris.127.0.0.1.nip.io trino.127.0.0.1.nip.io
> EOF
> ```

Our service URLs will be:
| Service | URL |
|---|---|
| Apache Polaris | `http://polaris.127.0.0.1.nip.io:8080` |
| Trino Web UI | `http://trino.127.0.0.1.nip.io:8080` |

```bash
k3d cluster create lakehouse \
  --servers 1 \
  --agents 2 \
  -p "8080:80@loadbalancer"
```

Verify the cluster is up:

```bash
kubectl cluster-info
kubectl get nodes
```

---

## Step 2 — Install Apache Ozone

Apache Ozone is the storage foundation of our stack. It provides an S3-compatible API
(via its S3 Gateway service) that both Polaris and Trino will use to read/write Iceberg
table files.

### Add the Helm repo and install

```bash
helm repo add ozone https://apache.github.io/ozone-helm-charts/
helm repo update
```

For local development, we use a minimal `values.yaml` to reduce resource usage and expose
the S3 Gateway as a ClusterIP service (we'll use `kubectl port-forward` to access it locally):

```yaml
# ozone-values.yaml
scm:
  replicaCount: 1

om:
  replicaCount: 1

datanode:
  replicaCount: 3   # minimum for block placement

s3g:
  replicaCount: 1

# Disable TLS for local dev
tls:
  enabled: false
```

```bash
helm install ozone ozone/ozone \
  --namespace ozone \
  --create-namespace \
  --values ozone-values.yaml \
  --wait --timeout 5m
```

Watch the pods come up:

```bash
kubectl get pods -n ozone -w
```

You should see pods for `scm` (Storage Container Manager), `om` (Ozone Manager),
`datanode-0/1/2`, and `s3g`.

### Create a bucket via the S3 API

We create the bucket using the AWS CLI against the Ozone S3 Gateway. This ensures the
bucket is owned by `testuser` — the same credentials Polaris and Trino will use — so
path resolution is guaranteed to work.

Make sure the port-forward is running in a separate terminal:

```bash
kubectl port-forward -n ozone svc/ozone-s3g-rest 9878:9878
```

Then create the bucket:

```bash
AWS_ACCESS_KEY_ID=testuser AWS_SECRET_ACCESS_KEY=testpassword \
  aws s3 mb s3://warehouse --endpoint-url http://localhost:9878
```

Verify it was created:

```bash
AWS_ACCESS_KEY_ID=testuser AWS_SECRET_ACCESS_KEY=testpassword \
  aws s3 ls --endpoint-url http://localhost:9878
```

You should see:

```
YYYY-MM-DD HH:MM:SS warehouse
```

### S3 credentials in non-secure mode

Because we're running Ozone without security (`ozone.security.enabled=false` — the default
for the Helm chart in local dev), the S3 Gateway accepts **any** access key and secret key.
There is no credential validation.

We'll use these placeholder values consistently in both Polaris and Trino:

```
OZONE_ACCESS_KEY=testuser
OZONE_SECRET_KEY=testpassword
```

> **For production:** enable Ozone security and use `ozone s3 getsecret -u <username>` to
> generate real per-user credentials backed by Kerberos.

---

## Step 3 — Install Apache Polaris

Apache Polaris is an open-source Iceberg REST catalog and an Apache Top-Level Project. It stores and
serves Iceberg table metadata and acts as the single source of truth for schema, partitioning,
and snapshot history. Trino (and any other Iceberg engine) talks to Polaris using the
standard Iceberg REST API.

### Add the Helm repo and install

```bash
helm repo add polaris https://downloads.apache.org/polaris/helm-chart
helm repo update
```

For local dev we override a few key values:

```yaml
# polaris-values.yaml

# Use in-memory persistence (good enough for local dev; loses state on pod restart)
persistence:
  type: in-memory

extraEnv:
  - name: POLARIS_BOOTSTRAP_CREDENTIALS
    value: "POLARIS,root,polaris-secret"
  - name: AWS_ACCESS_KEY_ID
    valueFrom:
      secretKeyRef:
        name: polaris-ozone-secret
        key: access-key
  - name: AWS_SECRET_ACCESS_KEY
    valueFrom:
      secretKeyRef:
        name: polaris-ozone-secret
        key: secret-key
  - name: AWS_REGION
    value: "us-east-1"
```

> **How credentials work:** `POLARIS_BOOTSTRAP_CREDENTIALS` sets the root principal on first boot
> (format: `realm,clientId,clientSecret`). The `AWS_*` env vars give Polaris the static S3
> credentials it uses when writing Iceberg metadata files to Ozone.
>
> We use `stsUnavailable: true` in the catalog's `storageConfigInfo` (see the next step) to tell Polaris that STS
> is not available and to use the static credentials directly — while still propagating the custom S3
> endpoint and path-style settings to the FileIO client.

Create the secret **before** installing Polaris:

```bash
kubectl create namespace polaris --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic polaris-ozone-secret \
  --namespace polaris \
  --from-literal=access-key=testuser \
  --from-literal=secret-key=testpassword
```

```bash
helm upgrade --install polaris polaris/polaris \
  --namespace polaris \
  --create-namespace \
  --values polaris-values.yaml \
  --version 1.3.0-incubating \
  --wait --timeout 3m
```

> **Note:** The Apache Polaris project graduated from the Incubator in February 2026, but
> the Helm chart hasn't been republished under a non-incubating version yet. Helm skips
> pre-release versions by default, so `--version 1.3.0-incubating` is required for now.
> Once a post-graduation chart is released, the version string will drop the `-incubating`
> suffix (e.g. `--version 1.4.0`), or you can omit `--version` entirely to get the latest.

Verify:

```bash
kubectl get pods -n polaris
```

Apply an Ingress so Polaris is reachable at `http://polaris.127.0.0.1.nip.io:8080`:

```yaml
# polaris-ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: polaris
  namespace: polaris
  annotations:
    traefik.ingress.kubernetes.io/router.entrypoints: web
spec:
  rules:
    - host: polaris.127.0.0.1.nip.io
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: polaris
                port:
                  number: 8181
```

```bash
kubectl apply -f polaris-ingress.yaml
```

Verify it's up:

```bash
curl http://polaris.127.0.0.1.nip.io:8080/api/catalog/v1/config
```

### Configure Polaris via the REST API

Polaris manages everything through its REST API. We need to:
1. Get an access token using the root credentials
2. Create a **principal** (service account for Trino)
3. Create a **catalog** (backed by Ozone storage)
4. Create a **namespace** inside the catalog

#### 1. Get an access token

```bash
TOKEN=$(curl -s -o /tmp/token.json -w "%{http_code}" \
  -X POST http://polaris.127.0.0.1.nip.io:8080/api/catalog/v1/oauth/tokens \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=root&client_secret=polaris-secret&scope=PRINCIPAL_ROLE:ALL")

echo "HTTP $TOKEN"
TOKEN=$(jq -r '.access_token' /tmp/token.json)
echo "Token: $TOKEN"
```

#### 2. Create a Trino principal and credentials

Polaris returns credentials **only once** at creation time — capture them immediately:

```bash
CREDS=$(curl -s -X POST http://polaris.127.0.0.1.nip.io:8080/api/management/v1/principals \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -o /tmp/principal.json -w "%{http_code}" \
  -d '{
    "name": "trino-principal",
    "type": "SERVICE"
  }')

echo "HTTP $CREDS"
# $CREDS holds only the status code; the JSON body is in /tmp/principal.json
TRINO_CLIENT_ID=$(jq -r '.credentials.clientId' /tmp/principal.json)
TRINO_CLIENT_SECRET=$(jq -r '.credentials.clientSecret' /tmp/principal.json)

echo "Trino Client ID:     $TRINO_CLIENT_ID"
echo "Trino Client Secret: $TRINO_CLIENT_SECRET"
```

> **If you see HTTP 409**, the principal already exists (e.g. from a previous attempt) and the
> secret cannot be retrieved again. Delete it and recreate:
> ```bash
> curl -s -X DELETE http://polaris.127.0.0.1.nip.io:8080/api/management/v1/principals/trino-principal \
>   -H "Authorization: Bearer $TOKEN" \
>   -w "\nHTTP %{http_code}"
> ```
> Then re-run the block above.

Save these — you'll use them in the Trino Helm values.

#### 3. Create a catalog backed by Ozone

We create an **internal** Polaris catalog and configure its default storage to use our
Ozone S3 bucket.

```bash
curl -X POST http://polaris.127.0.0.1.nip.io:8080/api/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -w "\nHTTP %{http_code}" \
  -d '{
    "name": "ozone_catalog",
    "type": "INTERNAL",
    "properties": {
      "default-base-location": "s3://warehouse/iceberg"
    },
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": ["s3://warehouse/"],
      "endpoint": "http://ozone-s3g-rest.ozone.svc.cluster.local:9878",
      "endpointInternal": "http://ozone-s3g-rest.ozone.svc.cluster.local:9878",
      "stsUnavailable": true,
      "pathStyleAccess": true
    }
  }'
```

> **Note:** Use `s3://` (not `s3a://`) in Polaris catalog config. Trino uses `s3a://` when
> reading/writing files, but Polaris stores and validates locations using `s3://` internally.
> `storageType: S3` is the correct type for any S3-compatible storage including Ozone
>
> The critical fields in `storageConfigInfo` are:
> - `stsUnavailable: true` — tells Polaris not to call the AWS STS service for temporary
>   credentials (Ozone has no STS endpoint). Polaris will use the `AWS_*` environment credentials
>   directly instead.
> - `endpoint` / `endpointInternal` — the S3-compatible endpoint for Ozone, injected into the
>   `StorageAccessConfig.extraProperties` passed to `S3FileIO` so file writes go to Ozone.
> - `pathStyleAccess: true` — forces path-style requests (`host/bucket/key`) instead of
>   virtual-hosted style (`bucket.host/key`), which Ozone requires.

#### 4. Grant the principal access to the catalog

Polaris uses a three-tier RBAC model: **principal → principal role → catalog role → privileges**.
We need to wire all of these together:

```bash
# 4a. Create a principal role
curl -X POST "http://polaris.127.0.0.1.nip.io:8080/api/management/v1/principal-roles" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -w "\nHTTP %{http_code}" \
  -d '{"principalRole": {"name": "trino-role"}}'

# 4b. Create a catalog role inside ozone_catalog
curl -X POST "http://polaris.127.0.0.1.nip.io:8080/api/management/v1/catalogs/ozone_catalog/catalog-roles" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -w "\nHTTP %{http_code}" \
  -d '{"catalogRole": {"name": "trino-catalog-role"}}'

# 4c. Grant CATALOG_MANAGE_CONTENT privilege to the catalog role
curl -X PUT "http://polaris.127.0.0.1.nip.io:8080/api/management/v1/catalogs/ozone_catalog/catalog-roles/trino-catalog-role/grants" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -w "\nHTTP %{http_code}" \
  -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}'

# 4d. Assign the catalog role to the principal role
curl -X PUT "http://polaris.127.0.0.1.nip.io:8080/api/management/v1/principal-roles/trino-role/catalog-roles/ozone_catalog" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -w "\nHTTP %{http_code}" \
  -d '{"catalogRole": {"name": "trino-catalog-role"}}'

# 4e. Assign the principal role to the trino principal
curl -X PUT "http://polaris.127.0.0.1.nip.io:8080/api/management/v1/principals/trino-principal/principal-roles" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -w "\nHTTP %{http_code}" \
  -d '{"principalRole": {"name": "trino-role"}}'
```

Each command should return `HTTP 201`. A `409` on steps 4a or 4b means the role already
exists (e.g. from a previous attempt) — that's fine, just continue to the next step.

#### 5. Create a namespace

```bash
curl -X POST "http://polaris.127.0.0.1.nip.io:8080/api/catalog/v1/ozone_catalog/namespaces" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -w "\nHTTP %{http_code}" \
  -d '{
    "namespace": ["demo"],
    "properties": {
      "location": "s3://warehouse/iceberg/demo"
    }
  }'
```

---

## Step 4 — Install Trino

Trino is our SQL query engine. We configure it with two things:
1. An Iceberg connector catalog that points to Polaris as the REST catalog
2. S3 file system settings pointing to Ozone

### Prepare the Helm values

```yaml
# trino-values.yaml

image:
  tag: "480"   # latest stable version at time of writing

server:
  workers: 1   # single worker is enough for local dev

# Allow Traefik's X-Forwarded-For headers (required when running behind an ingress proxy)
additionalConfigProperties:
  - http-server.process-forwarded=true

coordinator:
  resources:
    requests:
      memory: "1Gi"
      cpu: "500m"
    limits:
      memory: "2Gi"
      cpu: "1"

worker:
  resources:
    requests:
      memory: "2Gi"
      cpu: "500m"
    limits:
      memory: "4Gi"
      cpu: "1"

service:
  type: ClusterIP
  port: 8080

# Define the Iceberg catalog backed by Polaris
additionalCatalogs:
  lakehouse: |
    connector.name=iceberg
    iceberg.catalog.type=rest
    iceberg.rest-catalog.uri=http://polaris.polaris.svc.cluster.local:8181/api/catalog
    iceberg.rest-catalog.warehouse=ozone_catalog
    iceberg.rest-catalog.security=OAUTH2
    # For local dev: credential shorthand (clientId:clientSecret)
    iceberg.rest-catalog.oauth2.credential=<TRINO_CLIENT_ID>:<TRINO_CLIENT_SECRET>
    iceberg.rest-catalog.oauth2.server-uri=http://polaris.polaris.svc.cluster.local:8181/api/catalog/v1/oauth/tokens
    iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
    fs.native-s3.enabled=true
    s3.endpoint=http://ozone-s3g-rest.ozone.svc.cluster.local:9878
    s3.path-style-access=true
    s3.aws-access-key=testuser
    s3.aws-secret-key=testpassword
    s3.region=us-east-1
```

Replace the placeholders:
- `<TRINO_CLIENT_ID>` / `<TRINO_CLIENT_SECRET>` — from Step 3 (Polaris principal credentials)

### Add the repo and install

```bash
helm repo add trino https://trinodb.github.io/charts/
helm repo update

helm install trino trino/trino \
  --namespace trino \
  --create-namespace \
  --values trino-values.yaml \
  --wait --timeout 3m
```

Verify all pods are running:

```bash
kubectl get pods -n trino
```

You should see a `trino-coordinator-*` and `trino-worker-*` pod.

Apply an Ingress so the Trino UI is reachable at `http://trino.127.0.0.1.nip.io:8080`:

```yaml
# trino-ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: trino
  namespace: trino
  annotations:
    traefik.ingress.kubernetes.io/router.entrypoints: web
spec:
  rules:
    - host: trino.127.0.0.1.nip.io
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: trino
                port:
                  number: 8080
```

```bash
kubectl apply -f trino-ingress.yaml
```
Open http://trino.127.0.0.1.nip.io:8080 in your browser to see the Trino Web UI.

---

## Step 5 — End-to-End Test

Time to put it all together. We'll exec into the Trino coordinator pod and use the
built-in Trino CLI to create an Iceberg table, insert data, and query it back.

```bash
TRINO_POD=$(kubectl get pod -n trino \
  -l app.kubernetes.io/name=trino,app.kubernetes.io/component=coordinator \
  -o jsonpath='{.items[0].metadata.name}')

kubectl exec -n trino -it $TRINO_POD -- trino \
  --server http://localhost:8080 \
  --catalog lakehouse \
  --schema demo
```

Inside the Trino CLI:

```sql
-- Create an Iceberg table in the 'demo' namespace
CREATE TABLE lakehouse.demo.events (
    event_id   BIGINT,
    event_type VARCHAR,
    user_id    BIGINT,
    created_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(created_at)']
);

-- Insert some rows
INSERT INTO lakehouse.demo.events VALUES
    (1, 'page_view',  101, TIMESTAMP '2024-10-01 10:00:00 UTC'),
    (2, 'click',      102, TIMESTAMP '2024-10-01 11:30:00 UTC'),
    (3, 'purchase',   101, TIMESTAMP '2024-10-02 09:15:00 UTC');

-- Query the data
SELECT event_type, COUNT(*) AS cnt
FROM lakehouse.demo.events
GROUP BY event_type
ORDER BY cnt DESC;
```

Expected output:

```
 event_type | cnt
------------+-----
 page_view  |   1
 click      |   1
 purchase   |   1
```

You can also verify the files are physically present in Ozone:

```bash
kubectl exec -n ozone ozone-om-0 -- \
  ozone sh key list /s3v/warehouse
```

You'll see `.parquet` data files and a `metadata/` directory with Iceberg JSON metadata —
exactly what you'd see in S3 with a real cloud deployment.

---

## Tear Down

When you're done:

```bash
k3d cluster delete lakehouse
```

This destroys everything — the Kubernetes cluster, all Helm releases, and all data. Since
we used in-memory persistence for Polaris and ephemeral storage for Ozone, nothing leaks
onto your filesystem.

---

## Summary

In this tutorial we built a complete open data lakehouse locally using:

1. **k3d** to create a throwaway Kubernetes cluster in Docker
2. **Apache Ozone** as an S3-compatible object store (installed via Helm)
3. **Apache Polaris** as the Iceberg REST catalog (installed via Helm)
4. **Trino** as the SQL query engine (installed via Helm, configured to use Polaris + Ozone)

The entire stack runs on open standards (Iceberg REST API, S3 API) which means you can
swap any layer for a compatible alternative without changing the others. That portability
is the real value of this architecture.

---

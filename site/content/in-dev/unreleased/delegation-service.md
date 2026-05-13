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
title: Delegation Service
type: docs
weight: 430
---

A Delegation Service (D.S.) is a service that works alongside Polaris, either driving Polaris from outside or running inside the Polaris deployment to do work on its behalf. It can be deployed in one of two modes depending on who runs it and which way the calls flow:

- **Pull**: the delegation service runs outside Polaris (e.g. a scheduled compaction or snapshot-expiration job) and calls Polaris over REST to fetch policies, table metadata, etc.
- **Push**: the delegation service is co-deployed with Polaris, inside the same security boundary. Polaris invokes it for heavy workloads that would otherwise degrade the Polaris service, such as intensive network calls, large I/O operations, or compute-heavy tasks. The delegation service is hidden behind the Polaris deployment; clients cannot reach it and do not need to know whether one is configured or which implementation is in use.

The two modes solve different problems. Pull supports external systems that integrate with Polaris. Push lets a Polaris deployment offload internal work (e.g. table file purge on `DROP ... PURGE`, server-side scan planning) without changing the public API. A single deployment can use both.

```
Pull mode (delegation service is external):

  ┌──────────────────────┐    REST (pull)     ┌────────────┐
  │ Delegation service   │ ─────────────────► │  Polaris   │
  │ (compute engine,     │                    │            │
  │  maintenance job)    │                    └────────────┘
  └──────────────────────┘


Push mode (delegation service is internal, invisible to clients):

                        ┌── Polaris deployment ─────────────────┐
                        │                                       │
   ┌────────┐   REST    │   ┌──────────┐    internal   ┌──────┐ │
   │ Client │ ────────► │ ─►│ Polaris  │ ────────────► │ D.S. │ │
   └────────┘           │   └──────────┘               └──────┘ │
                        │                                       │
                        └───────────────────────────────────────┘
```

## Pull mode

Pull mode is the natural fit for **table maintenance services**: data compaction, snapshot expiration, orphan file removal, manifest rewriting, and similar background jobs. These services run on their own schedule, decide which tables to act on, and need policies and metadata from Polaris to drive that work.

In pull mode, the delegation service talks to Polaris **exclusively over REST APIs**: the Iceberg REST Catalog (IRC) endpoints
for table operations (load, commit, list, credential vending), and the Polaris REST endpoints for catalog-specific resources
such as policies and generic tables. There is no SDK, callback; every interaction is an outbound HTTP request. Authentication
uses OAuth2, the standard Polaris auth path; The full REST surface is in the [API specs](#api-specs).

### Example: external compaction service

```
1. POST /v1/oauth/tokens                            (auth)
2. GET  /v1/{cat}/namespaces/{ns}/tables             (discover tables)
3. GET  /polaris/v1/{cat}/applicable-policies        (pull policy)
       ?namespace={ns}&target-name={tbl}
       &policyType=system.data-compaction
4. If "enable": true, run compaction with the parameters from policy.content
5. Repeat on schedule
```

## Push mode

In push mode, the delegation service is co-deployed with the Polaris, in the same security boundary as Polaris itself. Polaris invokes it for heavy workloads that would otherwise degrade the Polaris service, such as intensive network calls, large I/O operations, or compute-heavy tasks. External clients cannot reach the delegation service directly, and they cannot tell whether or which one is deployed; Polaris remains the only public entry point.

### Properties

- **Same security boundary as Polaris.** The delegation service is reachable only by Polaris, deployed alongside it (e.g., separate pod within the same trust zone). It can be granted credentials and access that would be unsafe to vend to clients.
- **No public contract.** The wire protocol between Polaris and the delegation service is internal and may evolve. Clients see only the Polaris REST API.
- **Pluggable, opaque to clients.** Whether a delegation service is configured, and which implementation runs (e.g. an async worker for purge, an engine-aware planner for scan planning), is a deployment-time decision. The same client request behaves identically from the client's point of view regardless of which one is in use.

### Use cases

#### Drop with purge

When a client issues a drop with `purge=true`, Polaris must remove the catalog entry **and** delete the table's files.
Doing the file deletion in the request thread couples client latency to (potentially large) storage operations and saturates the outbound bandwidth of the Polaris server.
In push mode, Polaris records the drop and hands the purge job to the delegation service, which executes it asynchronously off the Polaris request path.
The client sees the table gone immediately; the file deletion completes behind the scenes.

#### Server-side scan planning

Server-side scan planning lets Polaris produce the file list for a query, instead of the engine reading table metadata directly. Two motivations: exposing non-Iceberg table formats through the Iceberg REST API, and sharing planning caches across queries. When enabled, Polaris invokes the delegation service to plan the scan, then returns the result over the IRC scan-planning endpoints. The client never knows whether the planning ran inside Polaris or in a delegation service.

## Choosing pull vs push

|                       | Pull                                                                          | Push                                                    |
|-----------------------|-------------------------------------------------------------------------------|---------------------------------------------------------|
| Where the D.S. runs   | Outside Polaris, owned by the caller                                          | Co-deployed with Polaris, in the same security boundary |
| Direction of calls    | D.S. → Polaris                                                                | Polaris → D.S.                                          |
| Visibility to clients | Visible (the D.S. *is* the client)                                            | Hidden; clients only see Polaris                        |
| Wire protocol         | Polaris REST (IRC + Polaris APIs)                                             | Internal, private to the deployment                     |
| Typical use cases     | Table maintenance jobs (compaction, snapshot expiration, orphan file cleanup) | Drop with purge, server-side scan planning              |

The two modes are independent: a deployment can use one, the other, or both.

## API specs

For the full REST surface, refer to the OpenAPI specs:

- [Iceberg REST Catalog](https://github.com/apache/polaris/blob/main/spec/iceberg-rest-catalog-open-api.yaml): IRC endpoints (table create, load, list, commit, credential vending).
- [Polaris management API](https://github.com/apache/polaris/blob/main/spec/polaris-management-service.yml): catalogs, principals, roles, grants.
- [Polaris policy API](https://github.com/apache/polaris/blob/main/spec/polaris-catalog-apis/policy-apis.yaml): policy CRUD and applicable-policies.
- [Polaris generic-tables API](https://github.com/apache/polaris/blob/main/spec/polaris-catalog-apis/generic-tables-api.yaml): generic-table CRUD.
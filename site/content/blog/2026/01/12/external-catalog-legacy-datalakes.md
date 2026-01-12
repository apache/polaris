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
title: "Mapping Legacy and Heterogeneous Datalakes in Apache Polaris"
date: 2026-01-12
author: Maninderjit Parmar
---
## Introduction

The data lake community and major engines such as Spark, Snowflake, and Trino are standardizing on the Iceberg REST catalog protocol for data discovery and access. While this shift provides the foundation for a modern lakehouse with transactions and centralized governance, many organizations still maintain significant volumes of data managed by legacy Hadoop/Hive catalogs or heterogeneous data lakes, resulting in siloed data environments with specialized query stacks.

For these organizations, migrating a legacy data lake to a modern REST-based catalog is costly, risky and complex due to security and compliance requirements as well as potential operational downtime. This post explores how to use Apache Polaris's read-only External Catalog feature to create a zero-copy metadata bridge across these datalakes. This capability allows organizations to project legacy and heterogeneous data lakes into the Iceberg REST ecosystem for access by any Iceberg-compatible engine without moving or copying the underlying data.

## Challenges of Existing Data Lake Systems

Integrating existing data lakes into a modern ecosystem involves several engineering hurdles. These challenges primarily stem from format heterogeneity, security/compliance requirements, and the operational complexity of keeping multiple systems in sync.

### Fragmented Formats and the Discovery Problem

A primary hurdle in this environment is that Iceberg is often not the native format for the entire pipeline. For instance, streaming platforms like Confluent or WarpStream use tools such as TableFlow to materialize Kafka topics as Iceberg tables, creating a continuous stream of new metadata and Parquet files. Similarly, teams often use utilities like Apache XTable or Delta Lake UniForm to generate Iceberg-compatible metadata for their existing Delta or Hudi tables without duplicating the underlying data.

This creates a discovery gap where the whole data asset is not discoverable by a single Iceberg catalog. Even after the metadata is generated, it must be registered with a central catalog to be useful for query engines. Relying on a traditional, pull-based catalog to monitor these diverse sources is operationally fragile. It often requires the catalog to maintain complex, long-lived credentials for every storage bucket and source system to perform crawling or polling operations.

### The Security and Reliability Burden

Traditional "pull" models also face security challenges. Granting a central catalog the permissions required to reach into multiple sub-engine/catalogs or across cloud provider accounts increases the risk of credential sprawl. Furthermore, in distributed systems where network issues are common, ensuring that a catalog correctly discovers every update without missing a commit is technically difficult.

## The Solution: A Push-Based, Stateless External Catalog

To address these challenges, Apache Polaris introduces the concept of an External Catalog. The External Catalog could be configured as a read-only "static facade" or as a read/write "passthrough facade" for federation to remote catalog. For the problem of mapping a legacy datalake, we use the External Catalog as the "static facade" and focus on it throughout the remaining blog. The architecture for it is designed around four core principles that simplify the integration of legacy data:

![External Catalog Architecture](/img/blog/2026/01/12/external-catalog-architecture.png "External Catalog Architecture showing data flow from producers through sync agents to Apache Polaris")

- **Push-Based Architecture**: Instead of the catalog polling for changes, external sync agents push metadata updates to Polaris via the [Notification API](https://github.com/apache/polaris/blob/main/spec/polaris-catalog-apis/notifications-api.yaml). This makes the catalog a passive recipient of state.

- **Stateless Credential Management**: By acting as a "static facade," Polaris does not need to store long-lived credentials for the source catalogs or databases. It only requires a definition of the allowed storage boundaries to vend short-lived, read-only scoped credentials to query engines.

- **Unidirectional Trust**: The security model relies on the producer environment pushing to Polaris. Polaris never initiates outbound connections to the source data lake, maintaining a strict security perimeter.

- **Idempotency and Weak Delivery Guarantees**: The [Notification API](https://github.com/apache/polaris/blob/main/spec/polaris-catalog-apis/notifications-api.yaml) is designed to tolerate at-least-once delivery. By using monotonically increasing timestamps within the notification payload, Polaris can safely reject older or duplicate updates, ensuring the catalog always reflects the most recent valid state of the data lake.

## Best Practices for Implementing a Sync Agent

In the push-based model of Apache Polaris, the catalog is passive. The responsibility for maintaining data freshness rests with the sync agent, the producer-side process that monitors your data lake and notifies Polaris of changes. To build a resilient and scalable sync agent, engineers should follow these three core principles.

### Monotonically Increasing Timestamps

The Polaris [Notification API](https://github.com/apache/polaris/blob/main/spec/polaris-catalog-apis/notifications-api.yaml) uses a timestamp-based ordering system to handle concurrent or out-of-order updates. When the sync agent sends a notification, it must include a timestamp that represents the "logical time" of the change. If a sync agent sends a notification with a timestamp that is older than one Polaris has already processed for that table, Polaris will return a `409 Conflict` error. This mechanism ensures that a stale update from a delayed network retry cannot overwrite the current state of your catalog.

### Design for Idempotency and Weak Delivery Guarantees

One of the most robust features of the [Notification API](https://github.com/apache/polaris/blob/main/spec/polaris-catalog-apis/notifications-api.yaml) is its ability to tolerate at-least-once delivery. In complex distributed systems, it is often difficult to guarantee that a message is delivered exactly once.

Because each notification contains the full location of a `metadata.json` file (provided by tools like XTable or TableFlow), operations are idempotent. Sync agents can retry failed requests without the risk of corrupting the state.

### Implement Event-Driven Change Detection

Instead of expensive directory crawling, agents should use S3/GCS event notifications or direct writer-integration to trigger updates as soon as new metadata files are written. By focusing on these events, the sync agent remains a lightweight bridge that keeps the catalog in sync with sub-second latency, without the overhead of a full data scan.

## Conclusion

The Apache Polaris [Notification API](https://github.com/apache/polaris/blob/main/spec/polaris-catalog-apis/notifications-api.yaml) provides a practical path to modernize legacy and heterogeneous data lakes without the risks of an all-or-nothing migration. By registering existing tables as metadata pointers, organizations gain standardized REST catalog access, centralized credentialed vending, and a simplified security model. This architectural bridge ensures that existing data assets remain first-class citizens in the modern AI and analytics era.


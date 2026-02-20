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
title: Configuration Reference
type: docs
weight: 200
---

This document provides a comprehensive reference for all Polaris configuration options.

All properties listed here are **runtime** properties and can be changed without rebuilding Polaris.

## Table of Contents

- [Features & Behavior](#features--behavior)
- [Authentication & Authorization](#authentication--authorization)
- [Storage & Credentials](#storage--credentials)
- [Persistence](#persistence)
- [Events](#events)
- [Operational](#operational)
- [Other](#other)

## Features & Behavior

### `polaris.features`

{{% include-config-section "flags-polaris_features" %}}

### `polaris.behavior-changes`

{{% include-config-section "flags-polaris_behavior_changes" %}}

## Authentication & Authorization

### `polaris.authentication`

{{% include-config-section "smallrye-polaris_authentication" %}}

### `polaris.authorization`

{{% include-config-section "smallrye-polaris_authorization" %}}

### `polaris.authorization.opa`

{{% include-config-section "smallrye-polaris_authorization_opa" %}}

### `polaris.oidc`

{{% include-config-section "smallrye-polaris_oidc" %}}

### `polaris.service-identity`

{{% include-config-section "smallrye-polaris_service_identity" %}}

## Storage & Credentials

### `polaris.storage`

{{% include-config-section "smallrye-polaris_storage" %}}

### `polaris.storage-credential-cache`

{{% include-config-section "smallrye-polaris_storage_credential_cache" %}}

### `polaris.credential-manager`

{{% include-config-section "smallrye-polaris_credential_manager" %}}

### `polaris.secrets-manager`

{{% include-config-section "smallrye-polaris_secrets_manager" %}}

### `polaris.file-io`

{{% include-config-section "smallrye-polaris_file_io" %}}

## Persistence

### `polaris.persistence`

{{% include-config-section "smallrye-polaris_persistence" %}}

### `polaris.persistence.cache`

{{% include-config-section "smallrye-polaris_persistence_cache" %}}

### `polaris.persistence.distributed-cache-invalidations`

{{% include-config-section "smallrye-polaris_persistence_distributed_cache_invalidations" %}}

### `polaris.persistence.nosql`

{{% include-config-section "smallrye-polaris_persistence_nosql" %}}

### `polaris.persistence.nosql.maintenance`

{{% include-config-section "smallrye-polaris_persistence_nosql_maintenance" %}}

### `polaris.persistence.nosql.maintenance.catalog`

{{% include-config-section "smallrye-polaris_persistence_nosql_maintenance_catalog" %}}

### `polaris.persistence.nosql.mongodb`

{{% include-config-section "smallrye-polaris_persistence_nosql_mongodb" %}}

### `polaris.persistence.relational.jdbc`

{{% include-config-section "smallrye-polaris_persistence_relational_jdbc" %}}

## Events

### `polaris.event-listener`

{{% include-config-section "smallrye-polaris_event_listener" %}}

### `polaris.event-listener.persistence-in-memory-buffer`

{{% include-config-section "smallrye-polaris_event_listener_persistence_in_memory_buffer" %}}

### `polaris.event-listener.aws-cloudwatch`

{{% include-config-section "smallrye-polaris_event_listener_aws_cloudwatch" %}}

## Operational

### `polaris.tasks`

{{% include-config-section "smallrye-polaris_tasks" %}}

### `polaris.async`

{{% include-config-section "smallrye-polaris_async" %}}

### `polaris.rate-limiter.filter`

{{% include-config-section "smallrye-polaris_rate_limiter_filter" %}}

### `polaris.rate-limiter.token-bucket`

{{% include-config-section "smallrye-polaris_rate_limiter_token_bucket" %}}

### `polaris.metrics`

{{% include-config-section "smallrye-polaris_metrics" %}}

### `polaris.iceberg-metrics.reporting`

{{% include-config-section "smallrye-polaris_iceberg_metrics_reporting" %}}

### `polaris.log`

{{% include-config-section "smallrye-polaris_log" %}}

### `polaris.readiness`

{{% include-config-section "smallrye-polaris_readiness" %}}

### `polaris.node`

{{% include-config-section "smallrye-polaris_node" %}}

## Other

### `polaris.realm-context`

{{% include-config-section "smallrye-polaris_realm_context" %}}


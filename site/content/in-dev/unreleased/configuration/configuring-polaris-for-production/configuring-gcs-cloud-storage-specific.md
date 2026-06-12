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
title: Configuring GCS Cloud Storage
linkTitle: Configuring GCS Cloud Storage
type: docs
weight: 600
---

This page provides guidance for configuring GCS Cloud Storage provider for use with Polaris. It covers credential vending, IAM roles, ACL requirements, and best practices to ensure secure and reliable integration.

All catalog operations in Polaris for Google Cloud Storage (GCS)—including listing, reading, and writing objects—are performed using credential vending, which issues scoped (vended) tokens for secure access.

Polaris requires both IAM roles and [Hierarchical Namespace (HNS)](https://docs.cloud.google.com/storage/docs/hns-overview) ACLs (if HNS is enabled) to be properly configured. Even with the correct IAM role (e.g., `roles/storage.objectAdmin`), access to paths such as `gs://<bucket>/idsp_ns/sample_table4/` may fail with 403 errors if HNS ACLs are missing for scoped tokens. The original access token may work, but scoped (vended) tokens require HNS ACLs on the base path or relevant subpath.

**Note:** HNS is not mandatory when using GCS for a catalog in Polaris. If HNS is not enabled on the bucket, only IAM roles are required for access. Always verify HNS ACLs in addition to IAM roles when troubleshooting GCS access issues with credential vending and HNS enabled.

## Hierarchical Namespace (HNS) Support

Polaris detects whether a target GCS bucket has HNS enabled automatically at table creation
time and at credential-vending time; no configuration flag is required, and the same catalog
can serve a mix of HNS and non-HNS buckets.

When HNS is detected on a write bucket, Polaris transparently handles two things:

- **Server-side pre-create.** Before delegating to Iceberg's `Catalog.createTable()`, Polaris
  uses the catalog service-account credentials to create the table's top-level folder
  structure (`<table>/`, `<table>/metadata/`, `<table>/data/`) via the GCS Storage Control
  API. This is required because HNS-enabled buckets treat folders as first-class resources
  that must exist before files can be written into them.
- **Vended-credential scope.** The downscoped credentials returned to clients include the
  minimal permissions needed to create deeper folders at write time (for partitioned
  ingestion, e.g. `data/year=2024/month=01/`): `storage.folders.create` and
  `storage.folders.get`, conditioned via access boundary expressions on the catalog's write
  locations. For non-HNS buckets no additional permissions are vended.

The least-privilege design is intentional: the folder rule references individual permissions
rather than the broader `roles/storage.folderAdmin` predefined role (which also grants
`setIamPolicy`, `delete`, `rename`, etc., none of which are needed for normal write paths).

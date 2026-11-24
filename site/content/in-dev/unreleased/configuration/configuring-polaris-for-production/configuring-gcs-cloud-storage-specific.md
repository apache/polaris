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

Polaris does not detect whether a bucket has HNS enabled, so vended write credentials always include the folder rule below. It only takes effect on HNS buckets: other buckets have no folder resources, so it grants nothing extra there. For each write location, for example a table at `gs://<bucket>/warehouse/ns/tbl/`:

- The write rule (`roles/storage.legacyBucketWriter`) covers `objects/warehouse/ns/tbl/` and `folders/warehouse/ns/tbl/` by prefix. Writers can create, rename and delete folders inside the table location, such as partition folders, as they can with directories on buckets without HNS.
- The vended credentials cannot create, rename or delete any folder outside the write locations, such as `folders/warehouse/ns/` or the folders of other tables. On HNS, writing an object needs its parent folders to exist, so before vending write credentials Polaris itself creates the missing parent folders of each write location (here `warehouse/` and `warehouse/ns/`). It does this with one folder-creation request per location, using the same identity it downscopes from.

A credential access boundary can only narrow the permissions of the identity that Polaris downscopes from: the configured `gcpServiceAccount`, or else the server's own credentials. That identity must therefore hold the `storage.folders.*` permissions on the bucket itself, for example through `roles/storage.objectAdmin`. Polaris also uses that identity's `storage.folders.create` permission directly to create parent folders, which can lie outside the table locations.

Creating parent folders never blocks credential vending. Buckets without HNS reject the request, and Polaris ignores that. If the request fails for another reason, for example because the identity lacks `storage.folders.create`, Polaris logs a warning once per bucket. On an HNS bucket, writes under the missing folder then fail with a permission error until the folder exists. Polaris tries again whenever it generates new credentials.

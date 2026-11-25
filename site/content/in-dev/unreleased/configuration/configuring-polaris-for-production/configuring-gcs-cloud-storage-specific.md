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

This page provides guidance for configuring GCS Cloud Storage provider for use with Polaris. It covers credential vending, IAM roles, buckets with Hierarchical Namespace (HNS), and best practices to ensure secure and reliable integration.

All catalog operations in Polaris for Google Cloud Storage (GCS)—including listing, reading, and writing objects—are performed using credential vending, which issues scoped (vended) tokens for secure access.

Polaris needs IAM roles on the bucket for the identity it downscopes from (see below). [Hierarchical Namespace (HNS)](https://docs.cloud.google.com/storage/docs/hns-overview) is not required: buckets with and without HNS are supported. The rest of this page describes what differs when HNS is enabled. When vended credentials get 403 errors on an HNS bucket, check that the identity Polaris downscopes from holds the `storage.folders.*` permissions and that `GCS_HNS_PARENT_FOLDER_CREATION_ENABLED` is `true`.

Polaris does not detect whether a bucket has HNS enabled, so vended write credentials always include the folder rule below. It only takes effect on HNS buckets: other buckets have no folder resources, so it grants nothing extra there. For each write location, for example a table at `gs://<bucket>/warehouse/ns/tbl/`:

- The write rule (`roles/storage.legacyBucketWriter`) covers `objects/warehouse/ns/tbl/` and `folders/warehouse/ns/tbl/` by prefix. Writers can create, rename and delete folders inside the table location, such as partition folders, as they can with directories on buckets without HNS.
- The vended credentials cannot create, rename or delete any folder outside the write locations, such as `folders/warehouse/ns/` or the folders of other tables. On HNS, writing an object needs its parent folders to exist, so before vending write credentials Polaris itself creates the missing parent folders of each write location (here `warehouse/` and `warehouse/ns/`). It sends one recursive folder-creation request per distinct parent folder, using the same identity it downscopes from. Credentials that may delete but not write, as requested by file cleanup tasks, create no folders.

A credential access boundary can only narrow the permissions of the identity that Polaris downscopes from: the configured `gcpServiceAccount`, or else the server's own credentials. That identity must therefore hold the `storage.folders.*` permissions on the bucket itself, for example through `roles/storage.objectAdmin`. Polaris also uses that identity's `storage.folders.create` permission directly to create parent folders, which can lie outside the table locations, unless parent folder creation is turned off (see below).

Creating parent folders never blocks credential vending. Buckets without HNS reject the request with HTTP 409, which Polaris ignores, or with 403 when the identity lacks `storage.folders.create`. Every other failure, including that 403, is logged as a warning each time Polaris generates new credentials. On an HNS bucket, writes under a missing folder then fail with a permission error until the folder exists; Polaris tries to create it again whenever it generates new credentials.

Parent folder creation is controlled by the feature flag `GCS_HNS_PARENT_FOLDER_CREATION_ENABLED` (default `true`). It applies to every GCS bucket in the realm and does not change the vended credentials. Turn it off only when no catalog in the realm writes to an HNS bucket, or when the parent folders are created by other means: otherwise writes to HNS buckets fail until their parent folders exist.

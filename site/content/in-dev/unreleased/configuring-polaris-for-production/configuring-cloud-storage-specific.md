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
title: Configuring Cloud Storage
linkTitle: Configuring Cloud Storage
type: docs
weight: 600
---

This page provides guidance for configuring cloud storage providers (such as Google Cloud Storage, Amazon S3, and Azure Blob Storage) for use with Polaris. 
It covers credential vending, IAM roles, ACL requirements, and best practices to ensure secure and reliable integration.

#### GCS

When using credential vending for Google Cloud Storage (GCS) with Apache Iceberg on
Polaris, ensure that both IAM roles and HNS ACLs (if HNS is enabled) are properly configured. Even with the correct IAM
role (e.g., `roles/storage.objectAdmin`), access to paths such as `gs://<bucket>/idsp_ns/sample_table4/` may fail with
403 errors if HNS ACLs are missing for scoped tokens. The original access token may work, but scoped (vended) tokens
require HNS ACLs on the base path or relevant subpath. Polaris does not require HNS to be enabled for basic operation,
but if HNS is enabled and credential vending is used, HNS ACLs are mandatory for scoped token access. Always verify HNS ACLs
in addition to IAM roles when troubleshooting GCS access issues with credential vending and HNS enabled.
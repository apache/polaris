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

This guide covers how to configure Google Cloud Storage (GCS) as a storage backend for Polaris catalogs, including credential vending, IAM configuration, and access control.

## Overview

Polaris uses **credential vending** to securely manage access to GCS objects. When you configure a catalog with GCS storage, Polaris issues scoped (vended) tokens with limited permissions and duration for each operation, rather than using long-lived credentials.

## Storage Configuration

When creating a Polaris catalog with GCS storage, you need to specify:

1. **Storage Type**: `GCS`
2. **Base Location**: The default GCS path for the catalog (e.g., `gs://your-bucket/catalogs/catalog-name`)
3. **Allowed Locations**: GCS paths where the catalog can read/write data

## IAM Configuration

### Service Account Permissions

The service account running Polaris (e.g., on Cloud Run) needs appropriate IAM roles to access GCS:

**Required IAM Roles:**
- `roles/storage.objectAdmin` - For read/write access to objects
- OR `roles/storage.objectViewer` + `roles/storage.objectCreator` - For more granular control

Grant the role at the bucket level:

```bash
gsutil iam ch serviceAccount:polaris-sa@project.iam.gserviceaccount.com:roles/storage.objectAdmin gs://your-bucket
```

### User Access Permissions

In addition to GCS IAM, users need Polaris catalog roles to access tables:

1. Create a catalog role with appropriate privileges:
   - `TABLE_READ_DATA` - Read table data
   - `TABLE_WRITE_DATA` - Write table data
   - `NAMESPACE_FULL_METADATA` - Access namespace/table metadata
2. Assign the catalog role to a principal role (e.g., `service_admin`)

This two-level permission model ensures both GCS access (via IAM) and Polaris access control (via catalog roles) are properly configured.

## Google Cloud Storage Configuration
The preferred GCS configuration to have Hierarchical Namespaces disabled on the bucket and Fine-grained ACLS for access control. 

## Troubleshooting

### 403 Forbidden Errors

1. Verify Polaris service account has IAM permissions on the bucket
2. Check that paths are within catalog's `allowedLocations`
3. Verify user has appropriate catalog role permissions

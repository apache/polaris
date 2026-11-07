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

## Server credentials

By default, Polaris uses one server-side GCP credential to mint vended tokens and to access storage. Catalogs never carry this credential. It comes from [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials): the `GOOGLE_APPLICATION_CREDENTIALS` environment variable pointing to a credentials file, or the service account attached to the workload.

### Named credentials

To use different credentials for different storages, enable the `RESOLVE_CREDENTIALS_BY_STORAGE_NAME` feature flag and set one credentials file per name:

```properties
polaris.features."RESOLVE_CREDENTIALS_BY_STORAGE_NAME"=true
polaris.storage.gcp.team-a.credentials-file=/var/run/secrets/team-a/credentials.json
```

Then set `storageName` on the GCS storage configuration of the catalog:

```json
{
  "storageType": "GCS",
  "allowedLocations": ["gs://team-a-bucket/"],
  "storageName": "team-a"
}
```

The file has the format `GOOGLE_APPLICATION_CREDENTIALS` points to, and must be readable by the Polaris server. In Kubernetes, mount it from a secret with the Helm chart's `extraVolumes` and `extraVolumeMounts`. Only service account key files (`service_account`) and Workload Identity Federation configuration files (`external_account`) are accepted. A Workload Identity Federation file holds no long-lived key.

The following rules apply:

- A storage without a `storageName` uses the default credentials.
- A `storageName` with no matching file fails. Polaris does not fall back to the default credentials.
- When the feature flag is disabled, `storageName` is ignored for credential selection.
- If `gcsServiceAccount` is also set, the named credentials are used to impersonate that service account.
- A `storageName` is refused while `GCS_PRINCIPAL_ATTRIBUTION_ENABLED` is true, because attribution uses a federated identity instead of these credentials.
- The names `token` and `lifespan` are reserved and cannot select credentials.
- A file is read the first time its name is used, and then kept until Polaris restarts. A bad file fails only the storages that use its name.
- When setting the property through an environment variable, a name containing a dash needs the literal property name as the variable name (`polaris.storage.gcp.team-a.credentials-file`). `POLARIS_STORAGE_GCP_TEAM_A_CREDENTIALS_FILE` alone does not define `team-a`.

Named credentials apply to the whole deployment, not to one realm. An administrator who can set `storageName` on a catalog in any realm can select any configured name, and can set the catalog's allowed locations and `gcsServiceAccount`. Only configure names whose access is appropriate for everyone who can administer catalogs on this server.

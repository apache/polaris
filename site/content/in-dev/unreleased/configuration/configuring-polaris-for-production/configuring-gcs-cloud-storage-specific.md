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

By default, Polaris uses one server-side GCP credential to mint vended tokens and to access storage. Catalogs never carry this credential. Polaris uses the first of these that is configured:

1. A credentials file, set with `polaris.storage.gcp.credentials-file`, or its content, set with `polaris.storage.gcp.credentials`. Set one of the two, not both. Only service account key files (`service_account`) and Workload Identity Federation configuration files (`external_account`) are accepted.
2. A static access token, set with `polaris.storage.gcp.token`. Polaris does not refresh it, so it stops working when it expires. It cannot be combined with the properties above.
3. [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials): the `GOOGLE_APPLICATION_CREDENTIALS` environment variable pointing to a credentials file, or the service account attached to the workload.

```properties
polaris.storage.gcp.credentials-file=/var/run/secrets/polaris/credentials.json
```

A credentials file or content is loaded the first time it is used, and then kept until Polaris restarts. To rotate a key, replace the file or the property and restart Polaris.

### Named credentials

To use different credentials for different storages, enable the `RESOLVE_CREDENTIALS_BY_STORAGE_NAME` feature flag and set one credentials file per name, or its content with `polaris.storage.gcp.<storage>.credentials`:

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

The credentials have the format `GOOGLE_APPLICATION_CREDENTIALS` points to, and a file must be readable by the Polaris server. Only service account key files (`service_account`) and Workload Identity Federation configuration files (`external_account`) are accepted. A Workload Identity Federation file holds no long-lived key.

The following rules apply:

- A storage without a `storageName` uses the credentials described in [Server credentials](#server-credentials).
- A `storageName` with no matching properties fails. Polaris does not fall back to the default credentials. Names are case-sensitive.
- When the feature flag is disabled, `storageName` is ignored for credential selection.
- If `gcsServiceAccount` is also set, the named credentials are used to impersonate that service account.
- A `storageName` is refused while `GCS_PRINCIPAL_ATTRIBUTION_ENABLED` is true. Attribution may use a federated identity instead of these credentials, so the name would apply to some requests only.
- A name's credentials are loaded the first time the name is used, and then kept until Polaris restarts. A name with both a file and content fails. A bad file or content fails only the storages that use its name, and is loaded again on the next use.

In Kubernetes, mount a credentials file from a secret with the Helm chart's `extraVolumes` and `extraVolumeMounts`, or project the content of a secret into the container with `extraEnv`:

```yaml
extraEnv:
  - name: polaris.storage.gcp.team-a.credentials
    valueFrom:
      secretKeyRef:
        name: polaris-gcp-team-a
        key: credentials.json
```

Kubernetes accepts the property name as the variable name. In a shell, use the uppercase form described in [Configuring Polaris]({{% ref "../configuring-polaris.md" %}}).

Named credentials apply to the whole deployment, not to one realm. An administrator who can set `storageName` on a catalog in any realm where `RESOLVE_CREDENTIALS_BY_STORAGE_NAME` is enabled can select any configured name, and can set the catalog's allowed locations and `gcsServiceAccount`. Only configure names whose access is appropriate for everyone who can administer catalogs on this server.

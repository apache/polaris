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
title: Federation
type: docs
weight: 703
---

Guides for federating Polaris with existing metadata services. Expand this section to select a
specific integration.

## Access control for federated catalogs

Polaris uses [role-based access control]({{% ref "../managing-security/access-control.md" %}})
(RBAC), in which privileges on securable objects are granted to catalog roles, and catalog roles
are assigned to principal roles. Grants normally target securable objects that exist in the Polaris
metastore. The namespaces, tables, and views of a federated catalog live in the remote system and
have no corresponding local entity, so by default they can only be secured at the catalog level,
not individually. The feature flags below enable finer-grained ("sub-catalog") RBAC on federated
catalogs. See the
[Configuration Reference]({{% ref "../configuration/configuration-reference.md" %}}) for the full
description of each flag.

### Prerequisite: enable federation

Federation must be enabled before the settings below apply (see the integration guides in this
section):

```properties
polaris.features."ENABLE_CATALOG_FEDERATION"=true
```

This feature is disabled by default.

### Enabling sub-catalog RBAC

`ENABLE_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS` (disabled by default) enables RBAC on the
individual namespaces, tables, and views of a federated catalog. When it is enabled, granting a
privilege on a federated namespace, table, or view — one that exists only in the remote catalog,
not in the Polaris metastore — is allowed: Polaris creates a *synthetic* entity in its metastore to
represent that securable and anchor the grant, instead of rejecting the operation because it does
not exist locally. These synthetic entities exist only to record the grants; the federated system
remains the source of truth for the metadata.

Set it realm-wide with a server feature flag:

```properties
polaris.features."ENABLE_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS"=true
```

Or override it for a single catalog with a catalog property, supplied when the catalog is created
(`--property`) or updated (`--set-property`):

```bash
polaris catalogs update <catalog> \
    --set-property polaris.config.enable-sub-catalog-rbac-for-federated-catalogs=true
```

### Governing per-catalog overrides

`ALLOW_SETTING_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS` (enabled by default) controls whether the
per-catalog property above may be set or changed:

```properties
polaris.features."ALLOW_SETTING_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS"=false
```

When set to `false`, any attempt to **set or change**
`polaris.config.enable-sub-catalog-rbac-for-federated-catalogs` on a catalog is rejected (in
`validateCatalogProperties`). Note this only blocks **future writes** of the property — it does
**not** change catalogs that already have it set: a stored catalog property takes precedence over
the realm-wide default (`resolveValue` checks the catalog property before the realm default), so a
catalog previously configured with the override keeps its stored value even after this guard flag
and the realm default are both `false`. To enforce the realm-wide default on such a catalog,
**remove its existing override** as well.

{{% alert title="Note" color="primary" %}}
Sub-catalog RBAC applies only to federated (external) catalogs. Enabling it creates synthetic local
entities to represent federated securables for grants; it does not import or copy metadata from the
remote catalog.
{{% /alert %}}

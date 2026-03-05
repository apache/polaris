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
title: flags-polaris_behavior_changes
build:
  list: never
  render: never
---

Internal behavior change configurations. These are unstable and may be removed.

##### `polaris.behavior-changes."ALLOW_NAMESPACE_CUSTOM_LOCATION"`

If set to true, allow namespaces with completely arbitrary locations. This should not affect credential vending.

- **Type:** `Boolean`
- **Default:** `false`
- **Catalog Config:** `polaris.config.namespace-custom-location.enabled`

---

##### `polaris.behavior-changes."ENTITY_CACHE_SOFT_VALUES"`

Whether or not to use soft values in the entity cache

- **Type:** `Boolean`
- **Default:** `false`

---

##### `polaris.behavior-changes."SCHEMA_VERSION_FALL_BACK_ON_DNE"`

If set to true, exceptions encountered while loading the VERSION table which appear to be caused by the VERSION table not existing will be interpreted as meaning that the schema version is currently 0.

- **Type:** `Boolean`
- **Default:** `true`

---

##### `polaris.behavior-changes."STORAGE_CONFIGURATION_MAX_LOCATIONS"`

How many locations can be associated with a storage configuration, or -1 for unlimited locations

- **Type:** `Integer`
- **Default:** `-1`

---

##### `polaris.behavior-changes."TABLE_OPERATIONS_MAKE_METADATA_CURRENT_ON_COMMIT"`

If true, BasePolarisTableOperations should mark the metadata that is passed into `commit` as current, and reuse it to skip a trip to object storage to re-construct the committed metadata again.

- **Type:** `Boolean`
- **Default:** `true`

---

##### `polaris.behavior-changes."VALIDATE_VIEW_LOCATION_OVERLAP"`

If true, validate that view locations don't overlap when views are created

- **Type:** `Boolean`
- **Default:** `true`

---

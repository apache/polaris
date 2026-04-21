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
title: flags-polaris_features
build:
  list: never
  render: never
---

Feature configurations for Polaris. These are stable, user-facing settings.

##### `polaris.features."ADD_TRAILING_SLASH_TO_LOCATION"`

When set, the base location for a table or namespace will have `/` added as a suffix if not present

- **Type:** `Boolean`
- **Default:** `true`
- **Catalog Config:** `polaris.config.add-trailing-slash-to-location`

---

##### `polaris.features."ALLOW_DROPPING_NON_EMPTY_PASSTHROUGH_FACADE_CATALOG"`

If enabled, allow dropping a passthrough-facade catalog even if it contains namespaces or tables. passthrough-facade catalogs may contain leftover entities when syncing with source catalog.In the short term these entities will be ignored, in the long term there will be method/background job to clean them up.

- **Type:** `Boolean`
- **Default:** `false`
- **Catalog Config:** `polaris.config.allow-dropping-non-empty-passthrough-facade-catalog`

---

##### `polaris.features."ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING"`

If set to true, allow credential vending for external catalogs.

- **Type:** `Boolean`
- **Default:** `true`
- **Catalog Config:** `polaris.config.enable.credential.vending`

---

##### `polaris.features."ALLOW_EXTERNAL_METADATA_FILE_LOCATION"`

If set to true, allows metadata files to be located outside the default metadata directory.

- **Type:** `Boolean`
- **Default:** `false`

---

##### `polaris.features."ALLOW_EXTERNAL_TABLE_LOCATION"`

If set to true, allows tables to have external locations outside the default structure.

- **Type:** `Boolean`
- **Default:** `false`
- **Catalog Config:** `polaris.config.allow.external.table.location`

---

##### `polaris.features."ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING"`

If set to true (default), allow credential vending for external catalogs. Note this requires ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING to be true first.

- **Type:** `Boolean`
- **Default:** `true`
- **Catalog Config:** `polaris.config.allow-federated-catalogs-credential-vending`

---

##### `polaris.features."ALLOW_INSECURE_STORAGE_TYPES"`

Allow usage of FileIO implementations that are considered insecure. Enabling this setting may expose the service to possibly severe security risks! This should only be set to 'true' for tests!

- **Type:** `Boolean`
- **Default:** `false`

---

##### `polaris.features."ALLOW_NAMESPACE_LOCATION_OVERLAP"`

If set to true, allow one namespace's location to reside within another namespace's location. This is only enforced within a parent catalog or namespace.

- **Type:** `Boolean`
- **Default:** `false`

---

##### `polaris.features."ALLOW_OPTIMIZED_SIBLING_CHECK"`

When set to true, Polaris will permit enabling the feature OPTIMIZED_SIBLING_CHECK for catalogs, this is done to prevent accidental enabling the feature in cases such as schema migrations, without backfill and hence leading to potential data integrity issues. This will be removed in 2.0.0 when polaris ships with the necessary migrations to backfill the index.

- **Type:** `Boolean`
- **Default:** `false`

---

##### `polaris.features."ALLOW_OVERLAPPING_CATALOG_URLS"`

If set to true, allows catalog URLs to overlap.

- **Type:** `Boolean`
- **Default:** `false`

---

##### `polaris.features."ALLOW_SETTING_S3_ENDPOINTS"`

If set to true (default), Polaris will permit S3 storage configurations to have custom endpoints. If set to false, Polaris will not accept catalog create and update requests that contain S3 endpoint properties.

- **Type:** `Boolean`
- **Default:** `true`

---

##### `polaris.features."ALLOW_SETTING_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS"`

If set to true (default), Polaris will allow setting or changing catalog property polaris.config.enable-sub-catalog-rbac-for-federated-catalogs.If set to false, Polaris will disallow setting or changing the above catalog property

- **Type:** `Boolean`
- **Default:** `true`

---

##### `polaris.features."ALLOW_SPECIFYING_FILE_IO_IMPL"`

Config key for whether to allow setting the FILE_IO_IMPL using catalog properties. Must only be enabled in dev/test environments, should not be in production systems.

- **Type:** `Boolean`
- **Default:** `false`

---

##### `polaris.features."ALLOW_TABLE_LOCATION_OVERLAP"`

If set to true, allow one table's location to reside within another table's location. This is only enforced within a given namespace.

- **Type:** `Boolean`
- **Default:** `false`
- **Catalog Config:** `polaris.config.allow.overlapping.table.location`

---

##### `polaris.features."ALLOW_UNSTRUCTURED_TABLE_LOCATION"`

If set to true, allows unstructured table locations.

- **Type:** `Boolean`
- **Default:** `false`
- **Catalog Config:** `polaris.config.allow.unstructured.table.location`

---

##### `polaris.features."ALLOW_WILDCARD_LOCATION"`

Indicates whether asterisks ('*') in configuration values defining allowed storage locations are processed as meaning 'any location'.

- **Type:** `Boolean`
- **Default:** `false`

---

##### `polaris.features."AZURE_RETRY_COUNT"`

Number of retry attempts for Azure API requests. Uses exponential backoff with jitter to handle transient failures.

- **Type:** `Integer`
- **Default:** `3`

---

##### `polaris.features."AZURE_RETRY_DELAY_MILLIS"`

Initial delay in milliseconds before first retry for Azure API requests. Delay doubles with each retry (exponential backoff).

- **Type:** `Integer`
- **Default:** `2000`

---

##### `polaris.features."AZURE_RETRY_JITTER_FACTOR"`

Jitter factor (0.0 to 1.0) applied to retry delays for Azure API requests. The jitter is applied as a random percentage of the computed exponential backoff delay. For example, 0.5 means up to 50%% random jitter will be added to each retry delay. Helps prevent thundering herd when multiple requests fail simultaneously.

- **Type:** `Double`
- **Default:** `0.5`

---

##### `polaris.features."AZURE_TIMEOUT_MILLIS"`

Timeout in milliseconds for Azure API requests. Prevents indefinite blocking when Azure endpoints are slow or unresponsive. Used internally by Azure storage integration for credential vending and other operations.

- **Type:** `Integer`
- **Default:** `15000`

---

##### `polaris.features."CLEANUP_ON_CATALOG_DROP"`

If set to true, clean up data when a catalog is dropped

- **Type:** `Boolean`
- **Default:** `false`
- **Catalog Config:** `polaris.config.cleanup.on.catalog.drop`

---

##### `polaris.features."CLEANUP_ON_NAMESPACE_DROP"`

If set to true, clean up data when a namespace is dropped

- **Type:** `Boolean`
- **Default:** `false`
- **Catalog Config:** `polaris.config.cleanup.on.namespace.drop`

---

##### `polaris.features."DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED"`

When enabled, Iceberg tables and views created without a location specified will have a prefix applied to the location within the catalog's base location, rather than a location directly inside the parent namespace. Note that this requires ALLOW_EXTERNAL_TABLE_LOCATION to be enabled, but with OPTIMIZED_SIBLING_CHECK enabled it is still possible to enforce the uniqueness of table locations within a catalog.

- **Type:** `Boolean`
- **Default:** `false`
- **Catalog Config:** `polaris.config.default-table-location-object-storage-prefix.enabled`

---

##### `polaris.features."DROP_WITH_PURGE_ENABLED"`

If set to true, allows tables to be dropped with the purge parameter set to true.

- **Type:** `Boolean`
- **Default:** `false`
- **Catalog Config:** `polaris.config.drop-with-purge.enabled`

---

##### `polaris.features."ENABLE_CATALOG_FEDERATION"`

If true, allows creating and using ExternalCatalogs containing ConnectionConfigInfos to perform federation to remote catalogs.

- **Type:** `Boolean`
- **Default:** `false`

---

##### `polaris.features."ENABLE_CREDENTIAL_RESET"`

Flag to enable or disable the API to reset principal credentials. Defaults to enabled, but service providers may want to disable it.

- **Type:** `Boolean`
- **Default:** `true`

---

##### `polaris.features."ENABLE_FINE_GRAINED_UPDATE_TABLE_PRIVILEGES"`

When true, enables finer grained update table privileges which are passed to the authorizer for update table operations

- **Type:** `Boolean`
- **Default:** `true`
- **Catalog Config:** `polaris.config.enable-fine-grained-update-table-privileges`

---

##### `polaris.features."ENABLE_GENERIC_TABLES"`

If true, the generic-tables endpoints are enabled

- **Type:** `Boolean`
- **Default:** `true`

---

##### `polaris.features."ENABLE_POLICY_STORE"`

If true, the policy-store endpoints are enabled

- **Type:** `Boolean`
- **Default:** `true`

---

##### `polaris.features."ENABLE_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS"`

When enabled, allows RBAC operations to create synthetic entities for entities in federated catalogs that don't exist in the local metastore.

- **Type:** `Boolean`
- **Default:** `false`
- **Catalog Config:** `polaris.config.enable-sub-catalog-rbac-for-federated-catalogs`

---

##### `polaris.features."ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING"`

If set to true, require that principals must rotate their credentials before being used for anything else.

- **Type:** `Boolean`
- **Default:** `false`

---

##### `polaris.features."ENTITY_CACHE_WEIGHER_TARGET"`

The maximum weight for the entity cache. This is a heuristic value without any particular unit of measurement. It roughly correlates with the total heap size of cached values. Fine-tuning requires experimentation in the specific deployment environment

- **Type:** `Long`
- **Default:** `104857600`

---

##### `polaris.features."ICEBERG_COMMIT_MAX_RETRIES"`

The max number of times to try committing to an Iceberg table

- **Type:** `Integer`
- **Default:** `4`
- **Catalog Config:** `polaris.config.iceberg-commit-max-retries`

---

##### `polaris.features."ICEBERG_ROLLBACK_COMPACTION_ON_CONFLICTS"`

Rollback replace snapshots created by compaction which have polaris.internal.conflict-resolution.by-operation-type.replace property set to rollback in their snapshot summary

- **Type:** `Boolean`
- **Default:** `false`
- **Catalog Config:** `polaris.config.rollback.compaction.on-conflicts.enabled`

---

##### `polaris.features."INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL"`

If set to true, principal name will be included in temporary subscoped credentials. Currently only AWS credentials are supported for which session name of the generated credentials will look like 'polaris-<principal>' rather than simple 'polaris'. Note that enabling this feature leads to degradation in temporary credential caching as catalog will no longer be able to reuse credentials for multiple principals.

- **Type:** `Boolean`
- **Default:** `false`

---

##### `polaris.features."LIST_PAGINATION_ENABLED"`

If set to true, pagination for APIs like listTables is enabled.

- **Type:** `Boolean`
- **Default:** `false`
- **Catalog Config:** `polaris.config.list-pagination-enabled`

---

##### `polaris.features."MAX_METADATA_REFRESH_RETRIES"`

How many times to retry refreshing metadata when the previous error was retryable

- **Type:** `Integer`
- **Default:** `2`

---

##### `polaris.features."OPTIMIZED_SIBLING_CHECK"`

When set, an index is used to perform the sibling check between tables, views, and namespaces. New locations will be checked against previous ones based on components, so the new location /foo/bar/ will check for a sibling at /, /foo/ and /foo/bar/%. In order for this check to be correct, locations should end with a slash. See ADD_TRAILING_SLASH_TO_LOCATION for a way to enforce this when new locations are added. Only supported by the JDBC metastore.

- **Type:** `Boolean`
- **Default:** `false`

---

##### `polaris.features."POLARIS_TASK_TIMEOUT_MILLIS"`

Polaris task expiry timeout (milliseconds). Older unfinished tasks may not be processed.

- **Type:** `Long`
- **Default:** `300000`

---

##### `polaris.features."PURGE_VIEW_METADATA_ON_DROP"`

If set to true, Polaris will attempt to delete view metadata files when a view is dropped.

- **Type:** `Boolean`
- **Default:** `true`
- **Catalog Config:** `polaris.config.purge-view-metadata-on-drop`

---

##### `polaris.features."RESOLVE_CREDENTIALS_BY_STORAGE_NAME"`

If set to true, resolve AWS credentials based on the storageName field of the storage configuration. When disabled, the default AWS credentials are used for all storages.

- **Type:** `Boolean`
- **Default:** `false`

---

##### `polaris.features."SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL"`

A comma-separated list of fields to include as session tags in AWS STS AssumeRole requests for credential vending. These tags appear in CloudTrail events, enabling correlation between catalog operations and S3 data access. An empty list (default) disables session tags entirely. Requires the IAM role trust policy to allow sts:TagSession action. Supported fields: realm, catalog, namespace, table, principal, roles, trace_id Note: each additional field may contribute to AWS STS packed policy size (max 2048 characters). Fields with long values (e.g. deeply nested namespaces) can cause STS policy size limit errors. Choose only the fields needed for your CloudTrail correlation requirements. WARNING: Including 'trace_id' effectively eliminates credential cache reuse because every request has a unique trace ID. This may significantly increase latency and STS API costs.

- **Type:** `List<String>`
- **Default:** `[]`

---

##### `polaris.features."SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION"`

If set to true, skip credential-subscoping indirection entirely whenever trying to obtain storage credentials for instantiating a FileIO. If 'true', no attempt is made to use StorageConfigs to generate table-specific storage credentials, but instead the default fallthrough of table-level credential properties or else provider-specific APPLICATION_DEFAULT credential-loading will be used for the FileIO. Typically this setting is used in single-tenant server deployments that don't rely on "credential-vending" and can use server-default environment variables or credential config files for all storage access, or in test/dev scenarios.

- **Type:** `Boolean`
- **Default:** `false`

---

##### `polaris.features."STORAGE_CREDENTIAL_CACHE_DURATION_SECONDS"`

How long to store storage credentials in the local cache. This should be less than STORAGE_CREDENTIAL_DURATION_SECONDS

- **Type:** `Integer`
- **Default:** `1800`

---

##### `polaris.features."STORAGE_CREDENTIAL_DURATION_SECONDS"`

The duration of time that vended storage credentials are valid for. Support for longer (or shorter) durations is dependent on the storage provider. GCS current does not respect this value.

- **Type:** `Integer`
- **Default:** `3600`

---

##### `polaris.features."SUPPORTED_CATALOG_CONNECTION_TYPES"`

The list of supported catalog connection types for federation

- **Type:** `List<String>`
- **Default:** `[ICEBERG_REST]`

---

##### `polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"`

The list of supported storage types for a catalog

- **Type:** `List<String>`
- **Default:** `[S3, AZURE, GCS]`
- **Catalog Config:** `polaris.config.supported.storage.types`

---

##### `polaris.features."SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES"`

The list of supported authentication types for catalog federation

- **Type:** `List<String>`
- **Default:** `[OAUTH, BEARER, SIGV4]`

---

##### `polaris.features."TABLE_METADATA_CLEANUP_BATCH_SIZE"`

Metadata batch size for tasks that clean up dropped tables' files.

- **Type:** `Integer`
- **Default:** `10`

---

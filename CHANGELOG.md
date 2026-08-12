<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Polaris Changelog

This changelog is used to give users and contributors condensed information about the contents of Polaris releases. 
Entries are grouped in sections like _Highlights_ or _Upgrade notes_, the provided sections can be adjusted
as necessary. Empty sections will not end up in the release notes. Contributors are encouraged to incorporate
CHANGELOG updates into their PRs when appropriate. Reviewers should be mindful of the impact of PRs and
request adding CHANGELOG notes for breaking (!) changes and possibly other sections as appropriate.

## [Unreleased]

### Highlights

### Upgrade notes

- Relational JDBC: schema version 6 corrects the `idx_locations` index on Postgres and CockroachDB
  (see Fixes). Fresh bootstraps use schema v6 automatically and get the right index. Because Polaris
  has no automated schema migrations, existing Postgres/CockroachDB deployments keep the old,
  ineffective index until an operator recreates it manually:
  ```sql
  DROP INDEX polaris_schema.idx_locations;
  CREATE INDEX idx_locations ON polaris_schema.entities USING btree (realm_id, catalog_id, location_without_scheme)
    WHERE location_without_scheme IS NOT NULL;
  ```
  H2 is unaffected.

- Relational JDBC: schema version 6 also declares `idx_grants_realm_grantee`,
  `idx_grants_realm_securable` and `idx_entities_catalog_id_id` on CockroachDB (see Fixes), which
  the Postgres and H2 schemas have declared since schema v4. Fresh bootstraps get them
  automatically; existing CockroachDB deployments, including any already at schema version 6,
  need them created manually:
  ```sql
  CREATE INDEX IF NOT EXISTS idx_grants_realm_grantee ON polaris_schema.grant_records (realm_id, grantee_id);
  CREATE INDEX IF NOT EXISTS idx_grants_realm_securable ON polaris_schema.grant_records (realm_id, securable_id);
  CREATE INDEX IF NOT EXISTS idx_entities_catalog_id_id ON polaris_schema.entities (catalog_id, id);
  ```
  Postgres and H2 are unaffected.

### Breaking changes

- Concurrent table commits that hit a stale sequence number now return a retryable `409` instead of a fatal `400`, for both single-table commits and `commitTransaction`.
- The Relational JDBC backend no longer creates its database schema during bootstrap: creating the
  schema is a privileged operation that belongs to a database administrator. Fresh installations
  must create the schema (by default `CREATE SCHEMA polaris_schema;` on PostgreSQL) before running
  the admin tool's `bootstrap` command. Existing deployments are unaffected — their schema already
  exists, and the shipped `currentSchema` default (`POLARIS_SCHEMA`) preserves the previous
  behavior on upgrade — with one exception:

  If your JDBC URL already sets `currentSchema`, check it before upgrading. Polaris previously
  qualified every query with `POLARIS_SCHEMA`, so the setting was ignored; it now selects the
  schema Polaris reads and writes, and the JDBC driver gives a value in the URL precedence over
  the shipped default. An upgrade would otherwise point Polaris away from its existing tables,
  and a subsequent `bootstrap` would create a second, empty set of tables in the other schema.
  Either remove the setting from the URL, or point it at the schema that already holds your
  Polaris tables.

### New Features

- Python CLI: `catalogs update` now supports `--no-sts` and `--no-kms` to toggle STS/KMS availability on an existing S3 catalog. Previously these were only settable at `catalogs create` time.
- Python CLI: added `gcp` as an external catalog authentication type for Iceberg REST federation, enabling CLI creation of GCP-authenticated catalogs such as BigLake without passing Google credential secrets through command-line flags.
- The database schema used by the Relational JDBC persistence backend is now configurable through standard datasource configuration: the JDBC driver's `currentSchema` connection property (defaulted to `POLARIS_SCHEMA` via `quarkus.datasource.jdbc.additional-jdbc-properties.currentSchema`) selects the schema, and the persistence layer is agnostic of the schema name. Also exposed as `persistence.relationalJdbc.additionalProperties.currentSchema` in the Helm chart.
- Python CLI: `catalogs create` and `catalogs update` now support `--storage-name` to set an optional name referencing a server-side storage configuration.

### Changes

- A metastore failure during authentication now returns a fixed `Service unavailable` message
  instead of naming the lookup that failed; the principal lookup previously returned `Unable to
  fetch principal entity`. The failing lookup is still named in the server log at `ERROR`, which
  operators can match to the client error through the request id, returned by default as
  `X-Request-ID` and printed by the default log format as `requestId`.
- `DROP_WITH_PURGE_ENABLED` now applies to Iceberg tables only. It previously also gated the
  metadata purge that a view drop performs internally, so with its default of `false` and
  `PURGE_VIEW_METADATA_ON_DROP` defaulting to `true`, dropping any view failed with HTTP 403 under
  the default configuration. A view drop is now governed by `PURGE_VIEW_METADATA_ON_DROP` alone,
  while the guard continues to protect a client-requested Iceberg table purge.
- Client-requested list page sizes are now bounded by a server-side maximum, configured with
  `LIST_PAGINATION_MAX_PAGE_SIZE` (default `100`, overridable per catalog via
  `polaris.config.list-pagination-max-page-size`). A request for a larger page is reduced to the
  maximum rather than rejected, since the Iceberg REST specification treats the requested page size
  as an upper bound.

### Deprecations

- Deprecated the `ADD_TRAILING_SLASH_TO_LOCATION` feature flag; Polaris now always appends a trailing slash to table and namespace base locations, so the key is accepted-but-ignored (a startup warning is emitted only when it is `false` in `polaris.features` defaults or realm overrides) and will be removed in a future release.

### Fixes

- Iceberg REST: renaming a table or view with a missing `source` or `destination` now returns `400 Bad Request` instead of `500 Internal Server Error`.
- Python CLI `catalogs create --type external` now validates `--storage-type` and `--default-base-location` up front, matching the behavior for internal catalogs and the flags' documented "(Required)" status. Previously, omitting either produced an opaque pydantic `ValidationError` at request-build time.
- Iceberg REST: server-side JSON processing failures (HTTP 500) now return the standard Iceberg
  error envelope (`{"error": {...}}`) instead of a flat `{"code", "message"}` body, so Iceberg
  clients can parse the response rather than failing on an off-schema shape.
- Python CLI `setup export` now represents namespace paths as lists of levels in namespace,
  policy, and namespace-privilege entries. This preserves namespace levels that contain dots during
  `setup apply`; apply remains compatible with existing dot-delimited configurations. Older CLI
  versions cannot apply the new export format.
- Python CLI `setup export` now writes each catalog's `policies` as a list of
  `{name, namespace, ...}` entries instead of the previous name-keyed mapping, preserving policies
  with the same name in different namespaces. The new export format cannot be applied by older CLI
  versions; use the exporting CLI version or newer for `setup apply`.
- Python CLI `setup export` now preserves user-defined properties on principal roles,
  so exported configurations restore that metadata during `setup apply`.
- Python CLI `setup export` now preserves user-defined properties on principals and catalog roles,
  so exported configurations restore that metadata during `setup apply`.
- Python CLI `setup apply` no longer double-encodes policy content emitted by `setup export`, so
  exported configurations containing policies can be restored.
- Python CLI `setup export` now includes nested namespaces and policy definitions from those
  namespaces. Previously, only top-level namespaces and their policies were exported.
- Python CLI `setup export` now exits with an error without emitting partial YAML if any required
  API read fails. Previously, individual failures were logged but the command printed incomplete
  configuration and exited with status 0.
- The Iceberg REST catalog now returns HTTP 500 (instead of 400) when a commit's outcome is unknown (`CommitStateUnknownException`), as required by the Iceberg REST spec. Previously, clients received `BadRequestException` for a commit that may have been applied, which could lead to unsafe retries.
- Fixed JDBC persistence under `SERIALIZABLE` isolation (e.g. CockroachDB default) so that a concurrent entity create that loses a unique-name race no longer returns the phantom new entity as a successful create. The conflicting row is now reported as `ENTITY_ALREADY_EXISTS` instead of fabricating the entity that was not persisted.
- Python CLI `setup` now preserves `endpoint_internal` and `sts_endpoint` during apply and export for S3 configuration
- Fixed a false-negative in the JDBC optimized location-overlap check (`OPTIMIZED_SIBLING_CHECK`). Ancestor locations stored in `location_without_scheme` without a trailing slash were not matched by the generated ancestor equality terms, allowing nested table/namespace locations to be created under existing prefixes. The query now emits both slash-terminated and non-slash-terminated prefix terms and uses a slash-terminated `LIKE` pattern for descendant matching.
- Python CLI `setup` now preserves the Azure `hierarchical` storage flag during apply and export
- Fixed policy detach on the relational JDBC backend silently doing nothing when the mapping's `parameters` changed in between. The delete's `WHERE` clause included the non-key `parameters` column, so a re-attach landing between the detach's lookup and its delete made the delete match zero rows while detach still reported success, leaving the policy attached. The delete is now keyed on the mapping's identity columns, matching the table's primary key and the transactional backend's behavior.
- Relational JDBC: the `idx_locations` index used by the optimized sibling check now matches the
  query that reads it. On Postgres and CockroachDB the index led with `parent_id`, while the overlap
  query filters `catalog_id`, so with `OPTIMIZED_SIBLING_CHECK` enabled every `CREATE TABLE` /
  `CREATE NAMESPACE` fell back to a realm-wide scan instead of the intended indexed lookup. A new
  schema version 6 creates the index on `(realm_id, catalog_id, location_without_scheme)`; H2 was
  already correct. Existing deployments need a manual index recreation — see Upgrade notes.
- A metastore failure while resolving a principal's roles during authentication now returns HTTP 503, as it already did when looking up the principal entity; previously it propagated unwrapped and was reported as HTTP 500. All three lookups now report `PolarisServiceUnavailableException` as the error `type`, where the principal entity lookup previously reported `ServiceUnavailableException` with the same status. All three also send `Retry-After: 0`; the Iceberg REST spec has a client retry a non-idempotent request only when that header is present.
- Relational JDBC: the CockroachDB schema now declares `idx_grants_realm_grantee`,
  `idx_grants_realm_securable` and `idx_entities_catalog_id_id` as of schema version 6; the
  Postgres and H2 schemas have carried them since schema v4. Without `idx_grants_realm_grantee`,
  loading the grants held by a principal, principal role or catalog role scans every grant record
  in the realm, because the `grant_records` primary key continues with the securable columns after
  `realm_id`. Existing CockroachDB deployments need a manual index creation — see Upgrade notes.
- Creating a namespace without an explicit location no longer fails with HTTP 400 when the
  catalog's `default-base-location` sits inside an allowed location instead of being one of
  them. For example, with allowed location `s3://b1` and `default-base-location` `s3://b1/d1`,
  `CREATE NAMESPACE ns` places the namespace at `s3://b1/d1/ns`, but the check expected it
  directly under an allowed location, at `s3://b1/ns`, and rejected it as a custom location even
  though the request asked for none. The namespace location is now compared against the
  catalog's `default-base-location`, which is what it is derived from.

### Commits

## [1.7.0]

### Upgrade notes

- Relational JDBC: a new schema version 5 makes the `events.catalog_id` column nullable. Events that
  are not scoped to a catalog (principal, policy, rate-limiting, etc.) now persist `NULL` instead of
  the placeholder string `__realm__` (which only ever existed in 1.6.0 release candidates). Fresh
  bootstraps use schema v5 automatically. Existing deployments on schema v3/v4 keep working — the
  server detects the older schema version and continues writing the legacy placeholder — but
  operators are encouraged to upgrade manually, since Polaris has no automated schema migrations:
  ```sql
  ALTER TABLE polaris_schema.events ALTER COLUMN catalog_id DROP NOT NULL;
  UPDATE polaris_schema.events SET catalog_id = NULL WHERE catalog_id = '__realm__';
  UPDATE polaris_schema.version SET version_value = 5 WHERE version_key = 'version';
  ```
  See the Relational JDBC metastore documentation for details.

### Breaking changes

- Removed the `--schema-version` (`-v`) option from the admin tool's `bootstrap` command. New realms
  are now always bootstrapped with the latest available schema version.
- The `MaintenanceService.performMaintenance()` signature now requires an explicit `OptionalLong overrideRunId` argument to supersede the latest unfinished maintenance run.
- Admin grant APIs now reject table-like privilege targets with an empty namespace. A table-like target without a namespace is considered invalid input.
- `PolarisPrincipal` now carries a generic `Map<String, Object> attributes` bag instead of the
  previous `Map<String, String> properties` and `Optional<String> token` fields. Three well-known
  attribute keys are defined as constants on the interface:

  - `PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY`: holds the `PrincipalEntity`;
  - `PolarisPrincipal.PRINCIPAL_ROLE_ALL_ATTRIBUTE_KEY`: holds a boolean indicating if the 
    `PRINCIPAL_ROLE:ALL` pseudo-role is present;
  - `PolarisPrincipal.JWT_ATTRIBUTE_KEY`: holds the raw JWT string.

  Use `PolarisPrincipal.getAttribute(key, type)` for type-safe access. The `Authenticator` interface
  now accepts a Quarkus `SecurityIdentity` instead of a `PolarisCredential`, and throws
  `AuthenticationFailedException` (mapped to HTTP 401) instead of Iceberg's
  `NotAuthorizedException`.
- Polaris does not include the `expiration-time` property anymore when vending credentials. This 
  property is not consumed by any known client and duplicates the properties specific to each
  storage provider, such as `s3.session-token-expires-at-ms` for S3.

### New Features
- Added AWS KMS decryption-key access through the `decryptionKeys` catalog storage configuration property.
- Added Kafka PolarisEventListener for publishing events to Kafka.
- Added GCS principal attribution for vended credentials (the GCP counterpart of AWS STS session tags). Set `GCS_PRINCIPAL_ATTRIBUTION_ENABLED=true` to activate; the feature flags `GCS_PRINCIPAL_ATTRIBUTION_WIF_AUDIENCE`, `GCS_PRINCIPAL_ATTRIBUTION_TOKEN_ISSUER`, and `GCS_PRINCIPAL_ATTRIBUTION_SIGNING_KEY_FILE` are then required (a missing value is a fatal configuration error). Also requires a `gcpServiceAccount` on the catalog StorageConfiguration. When enabled, credential vending chains a catalog-signed JWT through a Workload Identity Federation token exchange and service-account impersonation, so the Polaris principal appears in GCS Data Access audit logs (`serviceAccountDelegationInfo.principalSubject`) for any client. `GCS_PRINCIPAL_ATTRIBUTION_SIGNING_KEY_ID` sets the JWT `kid` for JWKS key rotation. Attribution is keyed per-principal in the credential cache; when disabled (default), GCP vending behaviour is unchanged.
- Added the `DEFAULT_UNIQUE_TABLE_LOCATION_ENABLED` feature flag (off by default). When enabled, a managed location generated for a table or view created without an explicit location is given a unique, unpredictable suffix, so that no two tables share a path prefix.
- Added the `ALLOW_CLIENT_SPECIFIED_TABLE_LOCATION` feature flag (on by default). When set to false, a caller-specified location (the `location` field, a `SetLocation` update, or the `write.data.path` / `write.metadata.path` properties) on a create-table (including a staged create-table request), create-view, update-table, replace-view, or commit-transaction request is rejected, forcing Polaris to manage all locations. Federated catalogs, committing an already staged create, and `register table` / `register view` are unaffected.
- Added `maintenance` support in Helm chart.
- Python CLI: added `--catalog-url` to specify a custom base URL for the Iceberg REST Catalog (IRC) API. Allows use with deployments that map a path (e.g. `/server1`) directly to the catalog root instead of the standard `/api/catalog`. See #4927.
- Added support for publishing histogram buckets for HTTP server request duration as configured SLO boundaries.
- Added an OpenTelemetry event listener for emitting Polaris audit events as OpenTelemetry log records.
- Added optional `sessionPolicy` field to `SigV4AuthenticationParameters` for catalog federation. When set, the IAM session policy JSON is attached to the STS AssumeRole request, allowing administrators to restrict vended credentials to only the required AWS services and actions (Principle of Least Privilege).
- Python CLI: added `--scheme` to specify URL scheme
- Added opt-in idempotency for `createTable` and `updateTable` in the Iceberg REST catalog. When enabled via `polaris.idempotency.enabled=true` (default `false`), a client-supplied `Idempotency-Key` header is embedded into the table entity and committed in the same transaction as the operation; a retry carrying the same key within the TTL window (`polaris.idempotency.ttl`, default `PT5M`) replays the original success instead of failing — with `AlreadyExists` for `createTable`, or with `CommitFailedException` for `updateTable` when the request's requirements no longer match the already-advanced table. When idempotency is enabled, the reuse window is advertised to clients through the `idempotency-key-lifetime` field of the `GET /v1/config` response.
- Python CLI: Added views and generic-tables support.

### Changes

- The admin tool's `bootstrap` command is now idempotent: bootstrapping a realm that is already
  bootstrapped is reported as "Realm '<realm>' is already bootstrapped; skipping." and no longer
  fails the command, making automated bootstrap jobs safe to re-run. Correspondingly,
  `PolarisMetaStoreManager.bootstrapPolarisService` now returns a result with
  `ReturnStatus.ENTITY_ALREADY_EXISTS` instead of throwing `IllegalArgumentException` when the
  root principal already exists. Existing credentials are never returned or altered.
- Authorization failure messages (HTTP 403 / `ForbiddenException` from `PolarisAuthorizerImpl`) now log the specific missing privilege(s) and the entity each was checked against server-side (at `INFO` level), e.g. `missing TABLE_CREATE on NAMESPACE 'ns1'`. The client-facing 403 response remains a generic message to avoid leaking authorization metadata to untrusted clients. Operators can correlate client errors to server logs using the existing `X-Request-ID` header (present in default log MDC as `requestId`).
- The field `clientSecret` of the Polaris management API type `ResetPrincipalRequest` is now using `format: password`. This does not change the wire format, but code generated from the OpenAPI may require downstream changes.

### Deprecations
- The `currentKmsKey` and `allowedKmsKeys` AWS storage configuration properties are deprecated. Use `encryptionKeys` instead.
- Deprecated `ALLOW_EXTERNAL_TABLE_LOCATION`. Use `ALLOW_EXTERNAL_METADATA_FILE_LOCATION` for external metadata file locations, including catalog config `polaris.config.allow.external.metadata.file.location`.

### Fixes

- Python CLI `setup` now preserves the catalog `storageName` field during export and apply, so named storage credential selection survives backup and migration round trips.
- The NoSQL persistence commit log (`Commits.commitLog`) no longer stops early when a commit's recent-ancestor tail is shorter than the internal fetch page size. With a `polaris.persistence.reference-previous-head-count` smaller than the page size, the natural-order commit log previously truncated at the first short tail because trailing null entries in the fetch page were treated as end-of-history, which could also drop still-referenced objects during maintenance.
- Python CLI REPL now shows a clear "Syntax error" message for malformed input instead of a generic "unexpected error" message.
- Python CLI `setup apply` now exits with an error after any setup operation fails, while still attempting the remaining operations. Previously, individual failures were logged but the command reported success and exited with status 0.
- Python CLI `tables list`, `tables get`, and `tables delete` commands now exit with status 1 when catalog API requests fail. Previously, these commands printed an error but exited successfully.
- Default table storage locations with object-storage prefixing enabled are now percent-encoded with UTF-8 instead of the JVM default charset. Previously the persisted location of a table under a non-ASCII namespace or table name depended on the platform default charset, so the same table could resolve to a different (or lossy) location on a non-UTF-8 JVM.
- Async task execution (table cleanup, manifest and batch file cleanup) now retries when a handler returns false on transient errors (e.g. IO or delete failures). Previously `false` was swallowed with only a warning log and the task was never retried via the existing retry mechanism.
- `TableCleanupTaskHandler` now clamps `TABLE_METADATA_CLEANUP_BATCH_SIZE` to at least 1. Previously a non-positive realm override caused an infinite loop (0) or `IllegalArgumentException` (<0) when splitting metadata files for cleanup.
- `ManifestFileCleanupTaskHandler` now handles Iceberg v2 delete manifests in addition to data manifests. Previously, `DROP TABLE PURGE` on a v2 table that had been updated via merge-on-read DML left position-delete files and their manifests as orphans in object storage; the cleanup task failed silently because `ManifestFiles.read()` rejects delete manifests.
- OPA authorizer now includes the realm identifier in the authorization context sent to OPA (`input.context.realm`). This ensures OPA policies can enforce tenant isolation across realms, preventing potential collisions if identical principal or resource names exist in different realms.
- Management API delete operations for principals, principal roles, catalog roles, and catalogs now return error messages that match the actual failure reason (for example, concurrent modification no longer reports a misleading protected-entity message).
- Python CLI `setup apply` now defaults to an `INTERNAL` catalog type when the `type` field is left blank or null in the setup config, instead of crashing with `AttributeError`

## [1.6.0]

### Upgrade notes

- Event listeners are now executed on a dedicated executor. **This executor does not propagate the original request's CDI context**; listeners that were improperly relying on that should instead manage their own CDI request scope from now on. Furthermore, two new configuration options were introduced to configure the executor: 
  - `polaris.event-listener.executor.pool-size` configures the thread pool size.
  - `polaris.event-listener.executor.queue-size` configures the queue size for pending events when all threads are busy.

### Breaking changes

- The `MaintenanceService.performMaintenance()` signature now requires an explicit `OptionalLong overrideRunId` argument to supersede the latest unfinished maintenance run.
- The REST layer now enforces stricter validation for entity names (including namespaces, tables, views, and generic tables). Requests containing invalid names will be rejected with an HTTP 400 error. Existing clients should verify and rename entities before upgrading if their names fall into the following forbidden categories:
    - Empty strings
    - Names consisting solely of `.` or `..`
    - Names containing control (invisible) characters
    - Names with leading or trailing whitespace
    - Names containing any of these characters: <code>/\:*?"<>|#+`</code>

### New Features

- Added `SESSION_NAME_FIELDS_IN_SUBSCOPED_CREDENTIAL` feature flag for AWS credential vending. Operators can now configure an ordered list of fields (`realm`, `catalog`, `namespace`, `table`, `principal`) to compose structured STS role session names (e.g. `p-acme-hr_catalog-employee-etl_writer`). Session names are sanitized and proportionally truncated to the AWS 64-character limit. When unset, existing `INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL` behaviour is preserved.
- Added `hostUsers` support in Helm chart.
- Added documentation for BigQuery Metastore Catalog federation. Build with `-PNonRESTCatalogs=BIGQUERY` to include the BigQueryMetastoreCatalog federation extension. See `site/content/in-dev/unreleased/federation/bigquery-metastore-federation.md`.
- Support for view registration has been added.
- Python client: added support for Python 3.14
- Added support for `register table` overwrite semantics in the Iceberg REST catalog flow (`overwrite=true`) for internal Polaris catalogs. With overwrite enabled, existing table pointers can be updated to a new metadata location while preserving default behavior for `overwrite=false`.
- Added `REGISTER_TABLE_OVERWRITE` authorization operation mapped to `TABLE_FULL_METADATA` for deterministic overwrite authorization.
- Added Polaris Spark 4.0 client.

### Changes

- Added REPL support to Polaris CLI.
- The `nosql maintenance-run` admin command now rejects a new run when the latest recorded maintenance run is still unfinished, unless the operator explicitly passes `--supersede-run=<run-id>`.
- Added version option to Polaris CLI.
- The token broker now builds the JWT `Algorithm` and `JWTVerifier` once per realm in the `TokenBrokerFactory` and reuses them across requests, instead of rebuilding them on every `verify()`/`sign()` call on the request-scoped broker. For deployments using file-based symmetric secrets, the secret is now read once per realm (at first use) rather than on every JWT operation; rotating the on-disk secret requires a restart.

### Fixes

- `polaris.storage.max-http-connections` (and related read/connect/acquisition/idle timeouts) now take effect for Iceberg S3FileIO clients used in table operations (previously only affected the STS client pool).
- Fixed a boundary condition in GCS downscoped credential generation (`GcpCredentialsStorageIntegration`). Locations without a trailing slash could previously grant access to sibling object prefixes via the generated CEL conditions for `resource.name` and list prefixes. Granted paths are now normalized to a directory prefix (with a trailing slash) before the CEL conditions are built, so sibling prefixes can no longer satisfy the `startsWith` checks.
- Fixed `NullPointerException` during `dropEntity` when an entity referenced by a grant had been concurrently removed (or purged). `lookupEntities` can return null entries for dropped entities; these are now skipped safely.
- `RateLimiterFilter` now returns an Iceberg-compatible `ErrorResponse` JSON body on HTTP 429, with `Content-Type: application/json`. Previously the body was empty, causing Iceberg REST clients to surface an opaque error.
- The admin tool `purge` command now prints the underlying exception stack trace to stderr when a purge fails unexpectedly, matching the `bootstrap` command. Previously a failed purge printed only a generic message, giving operators no diagnostic information.
- The access token carried in `PolarisCredential` is now redacted from its `toString()`, so it is no longer written to authentication logs (including at WARN level on failed authentication).
- JWT verification now validates the issuer claim (`"polaris"`) in addition to the active claim. Tokens signed with the same key but carrying a different issuer are now rejected.
- Renaming a table or view now maps concurrent-modification and resolution failures to meaningful HTTP status codes instead of HTTP 500. A concurrent modification of the source returns 503 (Service Unavailable, retryable) with a `Retry-After: 3` header indicating the client should wait 3 seconds before retrying; a source or target path that no longer resolves (concurrently dropped or replaced) returns 404 (Not Found). HTTP 409 is intentionally not used because the Iceberg REST rename endpoint reserves 409 for "the target already exists".
- Inheritable policy mapping inserts in the JDBC backend now use the active transaction connection, so they roll back correctly with the surrounding transaction.
- Generic table drop now accepts table-scoped `TABLE_DROP` privilege.
- Azure SAS tokens are now signed for the configured duration instead of a hardcoded 1 hour, so long jobs no longer fail with expired credentials.

## [1.5.0]

### Breaking changes

- The `ConnectionCredentials.of()` method now throws an exception when more than one expiration timestamp property is present in the credentials map. Only a single expiration timestamp is allowed per credentials bundle.
- Entity names (namespaces, tables, views, generic tables) submitted to the REST layer are now rejected with HTTP 400 if they are empty, contain a `/`, or have leading/trailing whitespace. Clients that were previously able to create such entities must rename them before upgrading.
- Fixed `renameTable` to return HTTP 204 (No Content) instead of 200, as per the Iceberg REST Catalog spec.

### New Features

- Added `envFrom` support in Helm chart.
- Added summarize subcommand to Polaris CLI.
- Added find and tables options to Polaris CLI.
- Added support for multiple event listeners. Set `polaris.event-listener.types` to a comma-separated list of event listener types to enable multiple event listeners.
- Added support for enabling only a subset of event types and event categories per event listener. Set `polaris.event-listener.`_`<name>`_`.enabled-event-types` or `polaris.event-listener.`_`<name>`_`.enabled-event-categories` to the list of event types or categories for the specified event listener to only consume the selected subset of events.
- Added support for **Apache Ranger** as an external authorizer (Beta).

### Changes

- Improved Python CLI error messages and exit codes for invalid arguments and configuration errors.
- Removed unused `PolarisAuthorizableOperation` values: `REVOKE_PRINCIPAL_GRANT_FROM_PRINCIPAL_ROLE`, `REVOKE_PRINCIPAL_ROLE_GRANT_FROM_PRINCIPAL_ROLE`, `LIST_GRANTS_ON_ROOT`, `ADD_PRINCIPAL_GRANT_TO_PRINCIPAL_ROLE`, `LIST_GRANTS_ON_PRINCIPAL`, `ADD_PRINCIPAL_ROLE_GRANT_TO_PRINCIPAL_ROLE`, `LIST_GRANTS_ON_PRINCIPAL_ROLE`, `ADD_CATALOG_ROLE_GRANT_TO_CATALOG_ROLE`, `REVOKE_CATALOG_ROLE_GRANT_FROM_CATALOG_ROLE`, `LIST_GRANTS_ON_CATALOG_ROLE`, `LIST_GRANTS_ON_CATALOG`, `LIST_GRANTS_ON_NAMESPACE`, `LIST_GRANTS_ON_TABLE`, `LIST_GRANTS_ON_VIEW`.
- Changed deprecated APIs in JUnit 5. This change will force downstream projects that pull in the Polaris test packages to adopt JUnit 6.
- Added client id collision check during reset.
- The ExternalCatalogFactory interface has been renamed to FederatedCatalogFactory. Its createCatalog() and createGenericCatalog() method signatures have been extended to include a `catalogProperties` parameter of type `Map<String, String>` for passing through proxy and timeout settings to federated catalog HTTP clients.

### Deprecations

- The configuration option `polaris.event-listener.type` is deprecated and will be removed later. Please use `polaris.event-listener.types` instead.

### Fixes

- Fixed native catalog credential vending paths (`loadCredentials` and `loadTable` with delegation) to re-validate locations against the *current* catalog `allowedLocations`. Previously these paths trusted persisted table entity locations, allowing stale credentials after an admin tightened allowed locations on a native catalog. (The federated path had a partial check.)

## [1.4.0]

### Upgrade notes

- The custom token-bucket based rate limiter has been replaced with Guava's rate limiter implementation.
- The Helm chart now includes a JSON schema file for easy validation of values files. Because types 
  are now validated, existing values files may need to be updated to match the new schema.

### Breaking changes

- The (Before/After)CommitViewEvent has been removed.
- The (Before/After)CommitTableEvent has been removed.
- The `PolarisMetricsReporter.reportMetric()` method signature has been extended to include a `receivedTimestamp` parameter of type `java.time.Instant`.
- The `ExternalCatalogFactory.createCatalog()` and `createGenericCatalog()` method signatures have been extended to include a `catalogProperties` parameter of type `Map<String, String>` for passing through proxy and timeout settings to federated catalog HTTP clients.
- Metrics reporting now requires the `TABLE_READ_DATA` privilege on the target table for read (scan) metrics and `TABLE_WRITE_DATA` for write (commit) metrics.
- The `REVOKE_CATALOG_ROLE_FROM_PRINCIPAL_ROLE` operation no longer requires the `PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE` privilege. Only `CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE` on the catalog role is now required, making revoke symmetric with assign. This allows catalog administrators to fully manage catalog role assignments without requiring elevated privileges on principal roles.

### New Features

- Added `deploymentAnnotations` support in Helm chart.
- Added KMS properties (optional) to catalog storage config to enable S3 data encryption.
- Added `topologySpreadConstraints` support in Helm chart.
- Added `priorityClassName` support in Helm chart.
- Added support for including principal name in subscoped credentials. `INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL` (default: false) can be used to toggle this feature. If enabled, cached credentials issued to one principal will no longer be available for others.
- Added per-field selection for AWS STS session tags in credential vending. The new `SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL` configuration accepts a comma-separated list of fields to include as session tags (supported: `realm`, `catalog`, `namespace`, `table`, `principal`, `roles`, `trace_id`). This replaces the previous `INCLUDE_SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL` and `INCLUDE_TRACE_ID_IN_SESSION_TAGS` boolean flags. Selecting only the fields needed helps avoid AWS STS packed policy size limit errors (2048 characters) caused by long namespace paths. Note: including `trace_id` disables credential caching, which may increase STS calls and latency.
- Added support for [Kubernetes Gateway API](https://gateway-api.sigs.k8s.io/) to the Helm Chart.
- Added `hierarchical` flag to `AzureStorageConfigInfo` to allow more precise SAS token down-scoping in ADLS when
  the [hierarchical namespace](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-namespace)
  feature is enabled in Azure.
- Relaxed `client_id`, `client_secret` regex/pattern validation on reset endpoint call
- Added support for S3-compatible storage that does not have KMS (use `kmsUavailable: true` in catalog storage configuration)
- Added support for storage-scoped AWS credentials, allowing different AWS credentials to be configured per named storage. Enable with the `RESOLVE_CREDENTIALS_BY_STORAGE_NAME` feature flag (default: false). Storage names can be set explicitly via the `storageName` field on storage configuration, or inferred from the first allowed location's host.
- Added support for persisting Iceberg metrics (ScanReport, CommitReport) to the database. Enable by setting `polaris.iceberg-metrics.reporting.type=persisting` in configuration. Metrics tables are included in the main JDBC schema.
- Added setup options to Polaris CLI.
- Added CockroachDB as a supported database for the relational JDBC persistence backend. Set `polaris.persistence.relational.jdbc.database-type` to `cockroachdb` to get started.

### Changes

- The `gcpServiceAccount` configuration value now affects Polaris behavior (enables service account impersonation). This value was previously defined but unused. This change may affect existing deployments that have populated this property.
- (Before/After)UpdateTableEvent is emitted for all table updates within a transaction.
- Added KMS options to Polaris CLI.
- Changed from Poetry to UV for Python package management.
- Exclude KMS policies when KMS is not being used for S3.
- Improved default KMS permission handling to better distinguish read-only and read-write access.

### Deprecations

- The configuration option `polaris.rate-limiter.token-bucket.window` is no longer supported and should be removed.
- `PolarisConfigurationStore` has been deprecated for removal.

### Fixes

- Fixed error propagation in drop operations (`dropTable`, `dropView`, `dropNamespace`). Server errors now return appropriate HTTP status codes based on persistence result instead of always returning NotFound
- Enable non-AWS STS role ARNs
- Helm chart: fixed a bug that prevented CORS settings to be properly applied. A new setting `cors.enabled` has been introduced in the chart as part of the fix.

## [1.3.0-incubating]

### Highlights

- Support for [Iceberg Metrics Reporting] has been introduced in Polaris. Out of the box, metrics can
  be printed to the logs by setting the `org.apache.polaris.service.reporting` logger level to `INFO` (it's
  set to `OFF` by default). Custom reporters can be implemented and configured to send metrics to
  external systems for further analysis and monitoring.
- Support for [Open Policy Agent (OPA)] integration has been added to Polaris. This enables delegating
  authorization decisions to external policy decision points, allowing organizations to centralize
  policy management and implement complex authorization rules. OPA integration can be enabled by setting
  `polaris.authorization.type=opa` in the Polaris configuration.

### Upgrade notes

- The legacy management endpoints at `/metrics` and `/healthcheck` have been removed. Please use the
  standard management endpoints at `/q/metrics` and `/q/health` instead.

### Breaking changes

- The EclipseLink Persistence implementation has been completely removed.
- The default request ID header name has changed from `Polaris-Request-Id` to `X-Request-ID`.

### New Features

- Added `--no-sts` flag to CLI to support S3-compatible storage systems that do not have Security Token Service available.
- Support credential vending for federated catalogs. `ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING` (default: true) was added to toggle this feature.
- Enhanced catalog federation with SigV4 authentication support, additional authentication types for credential vending, and location-based access restrictions to block credential vending for remote tables outside allowed location lists.

### Changes

- `client.region` is no longer considered a "credential" property (related to Iceberg REST Catalog API).
- Relaxed the requirements for S3 storage's ARN to allow Polaris to connect to more non-AWS S3 storage appliances. 
- Added checksum to helm deployment so that it will restart when the configmap has changed.
- Generic Table is no longer in beta and is generally-available.
- Added Windows support for Python client.

## [1.2.0-incubating]

### Upgrade Notes

- Amazon RDS plugin enabled, this allows polaris to connect to AWS Aurora PostgreSQL using IAM authentication.

### Breaking changes

- Python3.9 support will be dropped due to EOL

### New Features

- Added a finer grained authorization model for UpdateTable requests. Existing privileges continue to work for granting UpdateTable, such as `TABLE_WRITE_PROPERTIES`.
  However, you can now instead grant privileges just for specific operations, such as `TABLE_ADD_SNAPSHOT`
- Added a Management API endpoint to reset principal credentials, controlled by the `ENABLE_CREDENTIAL_RESET` (default: true) feature flag.
- The `ENABLE_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS` was added to support sub-catalog (initially namespace and table) RBAC for federated catalogs.
  The setting can be configured on a per-catalog basis by setting the catalog property: `polaris.config.enable-sub-catalog-rbac-for-federated-catalogs`.
  The realm-level feature flag `ALLOW_SETTING_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS` (default: true) controls whether this functionality can be enabled or modified at the catalog level.
- Added support for S3-compatible storage that does not have STS (use `stsUnavailable: true` in catalog storage configuration)
- Python client: added support for custom realm and header
- Python client: added support for policy management

### Changes

- The following APIs will now return the newly-created objects as part of the successful 201 response: createCatalog, createPrincipalRole, createCatalogRole. 

### Deprecations

- The property `polaris.active-roles-provider.type` is deprecated and has no effect anymore.
- The EclipseLink Persistence implementation has been deprecated since 1.0.0 and will be completely removed
  in 1.3.0 or in 2.0.0 (whichever happens earlier).
- The legacy management endpoints at `/metrics` and `/healthcheck` have been deprecated in 1.2.0 and will be
  completely removed in 1.3.0 or in 2.0.0 (whichever happens earlier). Please use the standard management
  endpoints at `/q/metrics` and `/q/health` instead.

### Fixes

- Fixed incorrect Azure expires at field for the credentials refresh response, leading to client failure via #2633

## [1.1.0-incubating]

Apache Polaris 1.1.0-incubating was released on September 19th, 2025.

- **Highlights**
  - **HMS Federation Support**: Added support for Hive Metastore (HMS) federation, enabling integration with existing Hive metastores.
  - **Modularized Federation**: Introduced modularized federation architecture to support multiple catalog types and improve extensibility.
  - **External Authentication**: Added comprehensive support for external identity providers including Keycloak integration and Helm chart configuration options.
  - **Python Client Distribution**: The Python client is now packaged and distributed as a proper Python package for easier installation and usage.
  - **Catalog Federation CLI**: Extended the CLI with support for managing federated catalogs, making it easier to configure and operate catalog federation.
  - **MinIO**: Added MinIO integration support with comprehensive getting started documentation.
- **New features**
  - Added Catalog configuration for S3 and STS endpoints. This also allows using non-AWS S3 implementations.
  - The realm-level feature flag `ALLOW_SETTING_S3_ENDPOINTS` (default: true) may be used to disable this
    functionality.
  - The `IMPLICIT` authentication type enables users to create federated catalogs without explicitly
    providing authentication parameters to Polaris. When the authentication type is set to `IMPLICIT`,
    the authentication parameters are picked from the environment or configuration files.
  - The `DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED` feature was added to support placing tables
    at locations that better optimize for object storage.
  - The `LIST_PAGINATION_ENABLED` (default: false) feature flag can be used to enable pagination
    in the Iceberg REST Catalog API.
  - The Helm chart now supports Pod Disruption Budgets (PDBs) for Polaris components. This allows users to define
    the minimum number of pods that must be available during voluntary disruptions, such as node maintenance.
  - Feature configuration `PURGE_VIEW_METADATA_ON_DROP` was added to allow dropping views without purging their metadata files.
  - Introduced S3 path-style access support for improved compatibility with S3-compatible storage systems.
  - Enhanced Python client with integration tests and improved error handling.
  - Introduced extensible pagination token implementation for better API performance.
  - Added support for `s3a` scheme in addition to existing S3 schemes.
  - Enhanced Helm chart with support for external authentication configuration and relational JDBC backend options.
  - Added comprehensive diagnostics and monitoring capabilities throughout the system.
  - Introduced bootstrap command options to specify custom schema files for database initialization.
  - Added refresh credentials endpoint configuration to LoadTableResponse for AWS, Azure, and GCP. Enabling
    automatic storage credential refresh per table on the client side. Java client version >= 1.8.0 is required.
    The endpoint path is always returned when using vended credentials, but clients must enable the
    refresh-credentials flag for the desired storage provider.
  - Added a Management API endpoint to reset principal credentials, controlled by the `ENABLE_CREDENTIAL_RESET` (default: true) feature flag.
- **Changes**
  - Polaris Management API clients must be prepared to deal with new attributes in `AwsStorageConfigInfo` objects.
  - S3 configuration property role-ARN is no longer mandatory.
- **Breaking changes**
  - Helm chart: the default value of the `authentication.tokenBroker.secret.symmetricKey.secretKey` property has changed
      from `symmetric.pem` to `symmetric.key`.
  - For migrations from 1.0.x to 1.1.x, users using JDBC persistence and wanting to continue using v1 schema, must ensure that they,
    run following SQL statement under `POLARIS_SCHEMA` to make sure version table exists:
      ```sql
    CREATE TABLE IF NOT EXISTS version (
         version_key TEXT PRIMARY KEY,
         version_value INTEGER NOT NULL
      );
      INSERT INTO version (version_key, version_value)
        VALUES ('version', 1)
      ON CONFLICT (version_key) DO UPDATE
                              SET version_value = EXCLUDED.version_value;
      COMMENT ON TABLE version IS 'the version of the JDBC schema in use';
    
    ALTER TABLE polaris_schema.entities ADD COLUMN IF NOT EXISTS location_without_scheme TEXT;
    ```
    - Please don't enable [OPTIMIZED_SIBLING_CHECK](https://github.com/apache/polaris/blob/740993963cb41c2c1b4638be5e04dd00f1263c98/polaris-core/src/main/java/org/apache/polaris/core/config/FeatureConfiguration.java#L346) feature configuration, once the above SQL statements are run. As it may lead to incorrect behavior, due to missing data for location_without_scheme column.
- **Deprecations**
  - The property `polaris.active-roles-provider.type` is deprecated for removal.
  - The `ActiveRolesProvider` interface is deprecated for removal.

## [1.0.1-incubating]

Apache Polaris 1.0.1-incubating was released on August 16th, 2025. It’s a maintenance release on the 1.0.0 release fixing a couple of issues on the Helm Chart:

- remove db-kind in Helm Chart
- add relational-jdbc to helm

## [1.0.0-incubating]

Apache Polaris 1.0.0-incubating was released on July 9th, 2025.

- **Highlights**
  - First release ready for real-world workloads after the public beta 0.9.0 
  - **Binary distribution** – first release with single downloadable .tgz or .zip artifact. 
  - **Helm Chart** – debut of an official Helm chart for seamless Kubernetes deployment 
- **New features & enhancements**
  - **Policy Store** — persistence with schema evolution, built‑in TMS policies (Data compaction, Snapshot expiry, etc) and REST CRUD endpoints 
  - **Postgres JDBC persistence** — native JDBC backend with robust support for concurrent changes. 
  - **Rollback Compaction on Conflicts** - makes Polaris smarter, to revert the compaction commits in case of crunch to let the writers who are actually adding or removing the data to the table succeed. In a sense treating compaction as always a lower priority process. 
  - **Enhanced runtime** — new runtime powered by Quarkus delivers out‑of‑the‑box Kubernetes readiness, quick startup, OIDC integration, and many other benefits. Polaris server and admin tool are now using Quarkus as a runtime framework. 
  - **HTTP caching via ETag** — the loadTable endpoint supports ETag, reducing bandwidth and improving perceived latency 
  - **Support for external identity providers (IdP)** — Polaris can now be its own IdP, delegate to an external IdP, or both 
  - **Snapshot filtering** – clients can choose to load only referenced snapshots 
  - **Catalog Federation (experimental)** – federate requests to an external Iceberg REST or Hadoop Catalog 
  - **Generic Tables (experimental)** — serve multiple table formats besides Iceberg tables; initial Spark 3.5 plugin supports Delta Lake 
  - **Event Listener framework (experimental)** — subscribe to catalog events (AfterTableCommitedEvent, BeforeViewCommitedEvent, etc)
- **Notable bug fixes**
  - **CLI and Python Client improvements** – Support for new features, CLI repair, changes to the update subcommand, and various fixes 
  - **Safe configurations** – Catalog-level Polaris configurations follow a strict naming convention to avoid name clashes with user-provided configuration entries. Legacy Polaris configuration names are still supported in 1.0 to allow existing deployments to migrate without rush. 
  - **TableOperations optimizations** – Changes to BasePolarisTableOperations result in less traffic to object storage during commits 
  - **Bounded entity cache** – The entity cache is now more memory-aware and less likely to lead to OOMs 
  - **Bootstrapping fixes** – Users can more easily bootstrap a new realm. Root credentials can be provided by the user or generated by Polaris (and returned to the user). 
- **Breaking changes**
  - **Server Configuration** – The format used to configure the Polaris service in 0.9 has changed with the migration to Quarkus and changes to configurations 
  - **Bootstrap Flow** – The bootstrap flow used in 0.9 has changed with the migration to Quarkus and the new admin tool

## [0.9.0-incubating]

Apache Polaris 0.9.0 was released on March 11, 2025 as the first Polaris release. Only the source distribution is available for this release.

[Unreleased]: https://github.com/apache/polaris/compare/apache-polaris-1.7.0...HEAD
[1.7.0]: https://github.com/apache/polaris/compare/apache-polaris-1.6.0...apache-polaris-1.7.0
[1.6.0]: https://github.com/apache/polaris/compare/apache-polaris-1.5.0...apache-polaris-1.6.0
[1.5.0]: https://github.com/apache/polaris/compare/apache-polaris-1.4.0...apache-polaris-1.5.0
[1.4.0]: https://github.com/apache/polaris/compare/apache-polaris-1.3.0-incubating...apache-polaris-1.4.0
[1.3.0-incubating]: https://github.com/apache/polaris/compare/apache-polaris-1.2.0-incubating...apache-polaris-1.3.0-incubating
[1.2.0-incubating]: https://github.com/apache/polaris/compare/apache-polaris-1.1.0-incubating...apache-polaris-1.2.0-incubating
[1.1.0-incubating]: https://github.com/apache/polaris/compare/apache-polaris-1.0.1-incubating...apache-polaris-1.1.0-incubating
[1.0.1-incubating]: https://github.com/apache/polaris/compare/apache-polaris-1.0.0-incubating...apache-polaris-1.0.1-incubating
[1.0.0-incubating]: https://github.com/apache/polaris/compare/apache-polaris-0.9.0-incubating...apache-polaris-1.0.0-incubating
[0.9.0-incubating]: https://github.com/apache/polaris/commits/apache-polaris-0.9.0-incubating
[Open Policy Agent (OPA)]: https://www.openpolicyagent.org/
[Iceberg Metrics Reporting]: https://iceberg.apache.org/docs/latest/metrics-reporting/
[Apache Ranger Authorization]: https://ranger.apache.org/

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
title: "Apache Polaris 1.8.0"
linkTitle: "1.8.0"
release_version: "1.8.0"
release_date: "2026-09-28"
weight: -10800
hide_summary: true
exclude_search: false
type: downloads
menus:
  main:
    parent: releases
    weight: -10800
    identifier: releases-1.8.0
---

Released on September 28th, 2026.

### Downloads

| Artifact                                                                                                                                                                              | PGP Sig                                                                                                                            | SHA-512                                                                                                                                  |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| [source tar.gz](https://dlcdn.apache.org/polaris/1.8.0/apache-polaris-1.8.0.tar.gz)                                                                                                   | [.asc](https://dlcdn.apache.org/polaris/1.8.0/apache-polaris-1.8.0.tar.gz.asc)                                                     | [.sha512](https://dlcdn.apache.org/polaris/1.8.0/apache-polaris-1.8.0.tar.gz.sha512)                                                     |
| [binary tgz](https://dlcdn.apache.org/polaris/1.8.0/polaris-bin-1.8.0.tgz)                                                                                                            | [.asc](https://dlcdn.apache.org/polaris/1.8.0/polaris-bin-1.8.0.tgz.asc)                                                           | [.sha512](https://dlcdn.apache.org/polaris/1.8.0/polaris-bin-1.8.0.tgz.sha512)                                                           |
| [binary zip](https://dlcdn.apache.org/polaris/1.8.0/polaris-bin-1.8.0.zip)                                                                                                            | [.asc](https://dlcdn.apache.org/polaris/1.8.0/polaris-bin-1.8.0.zip.asc)                                                           | [.sha512](https://dlcdn.apache.org/polaris/1.8.0/polaris-bin-1.8.0.zip.sha512)                                                           |
| [Spark 3.5 with Scala 2.12 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.8.0/polaris-spark-3.5_2.12-1.8.0-bundle.jar)                        | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.8.0/polaris-spark-3.5_2.12-1.8.0-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.8.0/polaris-spark-3.5_2.12-1.8.0-bundle.jar.sha512) |
| [Spark 3.5 with Scala 2.13 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.8.0/polaris-spark-3.5_2.13-1.8.0-bundle.jar)                        | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.8.0/polaris-spark-3.5_2.13-1.8.0-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.8.0/polaris-spark-3.5_2.13-1.8.0-bundle.jar.sha512) |
| [Spark 4.0 with Scala 2.13 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-4.0_2.13/1.8.0/polaris-spark-4.0_2.13-1.8.0-bundle.jar)                        | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-4.0_2.13/1.8.0/polaris-spark-4.0_2.13-1.8.0-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-4.0_2.13/1.8.0/polaris-spark-4.0_2.13-1.8.0-bundle.jar.sha512) |

### Release Notes

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
- PolarisAuthorizer inputs now reflect the intent-based authorization SPI more directly. Resource
  targets and parent paths no longer include Polaris's internal synthetic `ROOT` container. For
  example, catalog targets now have empty parent paths, root-scoped operations such as
  `LIST_CATALOGS` no longer include a synthetic `ROOT` target.
- PolarisAuthorizer requests with multiple intents are now evaluated one intent at a time. OPA
  deployments will observe this as separate OPA queries per intent, so existing Rego policies that
  depend on the previous combined-intent input shape may need to be updated. Same applies to other
  PolarisAuthorizer implementations.
- Internal JWTs minted before credentials-generation binding (tokens without the `polaris-cv` claim) can no longer be used as subject tokens in token exchange; they remain valid as bearer tokens until expiry. During a rolling upgrade, an old node may still mint claim-less tokens: exchanging such a token on any already-upgraded node fails with `invalid_grant`, so clients can see intermittent exchange failures until the last old node is gone; after that, rejection is consistent.

### New Features

- Semantic models now support dedicated privileges for listing, creating, reading, updating,
  and dropping. Privileges can be granted to catalog roles on individual models or at namespace
  or catalog scope, with separate controls for managing model grants.
- Python CLI: `catalogs update` now supports `--no-sts` and `--no-kms` to toggle STS/KMS availability on an existing S3 catalog. Previously these were only settable at `catalogs create` time.
- Python CLI: added `gcp` as an external catalog authentication type for Iceberg REST federation, enabling CLI creation of GCP-authenticated catalogs such as BigLake without passing Google credential secrets through command-line flags.
- Python CLI: added a global `--page-size` option to paginate list calls internally on Iceberg endpoints. Requires the server-side `LIST_PAGINATION_ENABLED` feature flag.
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
- `TokenBroker.verify` now returns `null` for tokens not recognized by the internal broker
  (instead of failing auth), so MIXED mode can delegate to other mechanisms. Exceptions from
  `verify` are forwarded as-is rather than mapped to auth failure or MIXED fallback.
- Client-requested list page sizes can now be bounded by a server-side maximum, configured with
  `LIST_PAGINATION_MAX_PAGE_SIZE` (overridable per catalog via
  `polaris.config.list-pagination-max-page-size`). It defaults to `-1`, meaning unlimited, so the
  maximum is opt-in. Once set, a request for a larger page is reduced to the maximum rather than
  rejected, since the Iceberg REST specification treats the requested page size as an upper bound.
  For local catalogs the maximum takes effect only when `LIST_PAGINATION_ENABLED` is true, since
  with pagination disabled the requested page size is ignored and the full result set is returned;
  for federated catalogs it always applies, because Polaris paginates those listings itself.
  Setting a maximum deviates from the Iceberg REST specification, which requires a request that
  does not supply a `pageToken` to receive the complete result with a null `next-page-token`: such
  a request is then truncated to the maximum and answered with a continuation token, so a client
  that does not follow continuations sees only the first page.
- Table commits whose base metadata is already stale now fail before the new metadata file is
  written, saving an object-storage write and delete per conflict and returning the `409` to the
  client sooner.

### Deprecations

- Deprecated the `ADD_TRAILING_SLASH_TO_LOCATION` feature flag; Polaris now always appends a trailing slash to table and namespace base locations, so the key is accepted-but-ignored (a startup warning is emitted only when it is `false` in `polaris.features` defaults or realm overrides) and will be removed in a future release.

### Fixes

- GCS credential vending no longer fails with HTTP 500 when a table's location or `write.data.path`
  / `write.metadata.path` points at a bucket root without a trailing slash (e.g. `gs://bucket`).
  Such a location parses to an empty path and previously triggered a `StringIndexOutOfBoundsException`
  while building the access-boundary rules; GCS now handles it like the AWS integration.
- Return HTTP 404 instead of 204 when a generic table or its catalog path disappears after resolution and before deletion.
- Deleting a semantic model now returns HTTP 404 instead of HTTP 500 when the model or its
  catalog path disappears after resolution and before the deletion is persisted.
- Return HTTP 404 instead of 500 when a policy or its catalog path disappears after resolution and before deletion.
- Iceberg REST: a malformed `pageToken` on the namespace, table and view list endpoints now returns
  `400 Bad Request` (`Invalid page token`) instead of `500 Internal Server Error`. Tokens that are
  valid Base64 but not a serialized page token (garbage, truncated, or produced by an incompatible
  Polaris version) previously escaped as Jackson decoding exceptions.
- Iceberg REST: when `X-Iceberg-Access-Delegation` resolves to remote signing (not implemented),
  either because `remote-signing` was requested alone or because `vended-credentials,remote-signing`
  was requested against a catalog that cannot vend credentials, the `400` response now explains the
  situation and what to do (`This catalog cannot vend credentials or sign requests; request without
  X-Iceberg-Access-Delegation and configure storage credentials on the client`) instead of the opaque
  `Unsupported access delegation mode: REMOTE_SIGNING`.
- Iceberg REST: renaming a table or view with a missing `source` or `destination` now returns `400 Bad Request` instead of `500 Internal Server Error`.
- Async file-cleanup tasks now bound how long they wait for object-store deletions via the new `polaris.tasks.file-deletion-timeout` (default 1h), so a stalled storage endpoint can no longer pin a task-executor thread indefinitely; a timeout is terminal for the current run rather than immediately retried, so it does not stack more deletions onto the stalled endpoint.
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
- Python CLI `setup apply --dry-run` now reports already-existing namespaces as skipped instead of proposed creations.
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
- Setting a namespace's `location` property no longer fails with HTTP 400. With custom namespace
  locations disabled, which is the default, every `updateNamespaceProperties` request carrying a
  `location` was rejected, including one that simply repeated the location the namespace already
  had. The error also named an expected location one level too deep: a namespace at `s3://b1/ns`
  was told it should be at `s3://b1/ns/ns`.
- Internal JWTs are bound to principal secret generation via `polaris-cv` (no secret material in the
  token). Credential-generation is enforced on token exchange; bearer verify is signature and claims
  only. Secrets-load failures during exchange return service unavailable.
- The Policy API now rejects an unknown `policyType` query parameter on `listPolicies` and `getApplicablePolicies` with HTTP 400. Previously an unrecognized value (for example `system.data_compaction`, misspelling `system.data-compaction` with an underscore) was silently treated as "no filter", so the request returned policies of every type with HTTP 200, and clients could not tell a filtered result from an unfiltered one. An absent or empty `policyType` still means "no filter", as the API specification allows.
- File cleanup tasks now issue batched object-storage deletes again. `CatalogUtil.deleteFiles`
  batches only when the `FileIO` is an `instanceof SupportsBulkOperations`, but the `FileIO` reaching
  the cleanup tasks is wrapped by `ExceptionMappingFileIO` and, on Azure, by
  `WasbTranslatingFileIO`. Neither wrapper declared the capability held by the wrapped `FileIO`, so
  the check always failed and every file was deleted individually. Both wrappers now propagate
  `SupportsBulkOperations` when the wrapped `FileIO` supports it, which affects every storage
  backend, since `S3FileIO`, `GCSFileIO`, `ADLSFileIO` and `HadoopFileIO` all implement
  `DelegateFileIO`.
- Async task retries no longer fail with a `NullPointerException` when the task entity has already been dropped by a previous attempt. Such a retry is now recognized as an already-completed task and exits cleanly, instead of exhausting all retry attempts and logging a `NullPointerException` on each one.

#### Security Fixes

This release fixes a security issue:
* [CVE-2026-97395](../../community/security-advisories/cve-2026-97395/)

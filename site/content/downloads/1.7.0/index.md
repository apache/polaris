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
title: "Apache Polaris 1.7.0"
linkTitle: "1.7.0"
release_version: "1.7.0"
release_date: "2026-08-02"
weight: -10700
hide_summary: true
exclude_search: false
type: downloads
menus:
  main:
    parent: releases
    weight: -10700
    identifier: releases-1.7.0
---

Released on August 2nd, 2026.

### Downloads

| Artifact                                                                                                                                                                              | PGP Sig                                                                                                                            | SHA-512                                                                                                                                  |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| [source tar.gz](https://dlcdn.apache.org/polaris/1.7.0/apache-polaris-1.7.0.tar.gz)                                                                                                   | [.asc](https://dlcdn.apache.org/polaris/1.7.0/apache-polaris-1.7.0.tar.gz.asc)                                                     | [.sha512](https://dlcdn.apache.org/polaris/1.7.0/apache-polaris-1.7.0.tar.gz.sha512)                                                     |
| [binary tgz](https://dlcdn.apache.org/polaris/1.7.0/polaris-bin-1.7.0.tgz)                                                                                                            | [.asc](https://dlcdn.apache.org/polaris/1.7.0/polaris-bin-1.7.0.tgz.asc)                                                           | [.sha512](https://dlcdn.apache.org/polaris/1.7.0/polaris-bin-1.7.0.tgz.sha512)                                                           |
| [binary zip](https://dlcdn.apache.org/polaris/1.7.0/polaris-bin-1.7.0.zip)                                                                                                            | [.asc](https://dlcdn.apache.org/polaris/1.7.0/polaris-bin-1.7.0.zip.asc)                                                           | [.sha512](https://dlcdn.apache.org/polaris/1.7.0/polaris-bin-1.7.0.zip.sha512)                                                           |
| [Spark 3.5 with Scala 2.12 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.7.0/polaris-spark-3.5_2.12-1.7.0-bundle.jar)                        | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.7.0/polaris-spark-3.5_2.12-1.7.0-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.7.0/polaris-spark-3.5_2.12-1.7.0-bundle.jar.sha512) |
| [Spark 3.5 with Scala 2.13 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.7.0/polaris-spark-3.5_2.13-1.7.0-bundle.jar)                        | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.7.0/polaris-spark-3.5_2.13-1.7.0-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.7.0/polaris-spark-3.5_2.13-1.7.0-bundle.jar.sha512) |
| [Spark 4.0 with Scala 2.13 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-4.0_2.13/1.7.0/polaris-spark-4.0_2.13-1.7.0-bundle.jar)                        | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-4.0_2.13/1.7.0/polaris-spark-4.0_2.13-1.7.0-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-4.0_2.13/1.7.0/polaris-spark-4.0_2.13-1.7.0-bundle.jar.sha512) |

### Release Notes

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

- Deprecated `ALLOW_EXTERNAL_TABLE_LOCATION`. Use `ALLOW_EXTERNAL_METADATA_FILE_LOCATION` for external metadata file locations, including catalog config `polaris.config.allow.external.metadata.file.location`.

### Fixes

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

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
title: "Apache Polaris 1.9.0"
linkTitle: "1.9.0"
release_version: "1.9.0"
release_date: "2026-11-14"
weight: -10900
hide_summary: true
exclude_search: false
type: downloads
menus:
  main:
    parent: releases
    weight: -10900
    identifier: releases-1.9.0
---

Released on November 14th, 2026.

### Downloads

| Artifact                                                                                                                                                                              | PGP Sig                                                                                                                            | SHA-512                                                                                                                                  |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| [source tar.gz](https://dlcdn.apache.org/polaris/1.9.0/apache-polaris-1.9.0.tar.gz)                                                                                                   | [.asc](https://dlcdn.apache.org/polaris/1.9.0/apache-polaris-1.9.0.tar.gz.asc)                                                     | [.sha512](https://dlcdn.apache.org/polaris/1.9.0/apache-polaris-1.9.0.tar.gz.sha512)                                                     |
| [binary tgz](https://dlcdn.apache.org/polaris/1.9.0/polaris-bin-1.9.0.tgz)                                                                                                            | [.asc](https://dlcdn.apache.org/polaris/1.9.0/polaris-bin-1.9.0.tgz.asc)                                                           | [.sha512](https://dlcdn.apache.org/polaris/1.9.0/polaris-bin-1.9.0.tgz.sha512)                                                           |
| [binary zip](https://dlcdn.apache.org/polaris/1.9.0/polaris-bin-1.9.0.zip)                                                                                                            | [.asc](https://dlcdn.apache.org/polaris/1.9.0/polaris-bin-1.9.0.zip.asc)                                                           | [.sha512](https://dlcdn.apache.org/polaris/1.9.0/polaris-bin-1.9.0.zip.sha512)                                                           |
| [Spark 3.5 with Scala 2.12 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.9.0/polaris-spark-3.5_2.12-1.9.0-bundle.jar)                        | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.9.0/polaris-spark-3.5_2.12-1.9.0-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.12/1.9.0/polaris-spark-3.5_2.12-1.9.0-bundle.jar.sha512) |
| [Spark 3.5 with Scala 2.13 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.9.0/polaris-spark-3.5_2.13-1.9.0-bundle.jar)                        | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.9.0/polaris-spark-3.5_2.13-1.9.0-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-3.5_2.13/1.9.0/polaris-spark-3.5_2.13-1.9.0-bundle.jar.sha512) |
| [Spark 4.0 with Scala 2.13 Client Jar](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-4.0_2.13/1.9.0/polaris-spark-4.0_2.13-1.9.0-bundle.jar)                        | [.asc](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-4.0_2.13/1.9.0/polaris-spark-4.0_2.13-1.9.0-bundle.jar.asc) | [.sha512](https://repo1.maven.org/maven2/org/apache/polaris/polaris-spark-4.0_2.13/1.9.0/polaris-spark-4.0_2.13-1.9.0-bundle.jar.sha512) |

### Release Notes

### Upgrade notes

### Highlights

- Table and view commits no longer delete newly written metadata when the metastore write outcome
  is unknown (e.g. a dropped connection during `commit()`); they still clean up on known failures.
  JDBC reports that unknown outcome as `PersistenceCommitStateUnknownException` (HTTP 500). A
  connection failure before `commit()` is a plain failure and the new metadata file is kept.
- Polaris now fully supports "external" principals, that is, principals that are not backed by an 
  entity in Polaris metastore. By enabling external principals, either globally or per-realm,
  Polaris now skips the principal entity metastore lookup. This means that synchronizing principals 
  between an external IDP and Polaris is not necessary anymore. To enable external principals, 
  set the `polaris.authentication.credential-mode` option to `external`. Note: external principals
  are not compatible with internal authentication and internal authorization; you must configure an
  external IDP and an external PDP, such as OPA or Ranger.
- S3 storage configurations gain an optional string field, `credentialVendingMechanism`. A catalog
  that leaves it empty uses the server's default mechanism, STS AssumeRole, and management API
  responses omit the field for it; `STS` selects the same mechanism explicitly and is the only value
  this release supports. Mechanisms are CDI beans discovered by their `@Identifier` at startup,
  and each one can validate the catalogs that select it at create and update. A realm lists the
  explicit mechanisms it accepts in the new `SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS` feature
  (default `[STS]`), enforced at catalog create and update and when the server builds the storage
  integration that vends credentials for a catalog. A catalog that names a mechanism this server
  does not provide is refused at create and update, and whenever a credential is vended for it,
  with "S3 credential vending mechanism `<id>` is not available in this server".

### Upgrade notes

- `SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS` lists the explicit mechanisms a realm accepts; an empty
  `credentialVendingMechanism` is always allowed. Startup reports a listed mechanism with no
  installed bean as a non-severe readiness warning. Startup fails (a severe readiness issue) when no
  `STS` mechanism is installed, because every catalog with an empty field depends on it.
- Rolling upgrades: a node still running an earlier release does not know `credentialVendingMechanism`.
  It ignores the field in a stored configuration and vends through STS, and it drops the field from
  the catalog create and update requests it serves. Keep S3 storage configurations unchanged during
  the upgrade.
- Polaris-managed AWS SDK clients now use Apache HttpClient 5, which disables HTTP
  `Expect: 100-continue` by default. Set `polaris.storage.expect-continue-enabled=true`
  to preserve the previous behavior. Iceberg S3 clients continue to use Apache HttpClient 4
  and retain their existing default.
- The `apache-polaris` Python package no longer depends on `boto3`, which it never used. Environments
  that relied on it being installed transitively must now install `boto3` explicitly.
- Relational JDBC: The per-schema-version runtime fallback has been removed. The migration to schema
  v6 is now **required** before starting this version of Polaris. The first request to any realm
  whose recorded schema version does not match what the binary expects will fail fast with a clear
  error message. See the [Relational JDBC metastore documentation] for the full upgrade path.
- New privilege `CATALOG_READ_CONFIG` (authorizable operation `GET_CATALOG_CONFIG`) is available for
  the Iceberg REST `GET /v1/config` endpoint. The endpoint hard-gate is **off by default** behind
  `polaris.features."ENFORCE_CATALOG_CONFIG_AUTHORIZATION"`, so upgrades do not 403 bootstrap. When
  enabled, callers need `CATALOG_READ_CONFIG` (or a catalog-level content privilege that subsumes
  it). A 200 still returns catalog properties (`defaults`) as today. Before enabling: grant
  `CATALOG_READ_CONFIG` and update Ranger/OPA policies for `GET_CATALOG_CONFIG`. Ranger currently
  maps that operation to the existing `catalog-properties-read` access type — do not enable the flag
  for Ranger until a dedicated `catalog-config-read` access type exists (or accept that coarse
  mapping). A production-readiness warning is emitted while the flag remains false. The default will
  not flip until client-facing catalog configuration is defined. Table- or namespace-scoped grants
  alone do not satisfy the catalog-path check used for `CATALOG_READ_CONFIG` once enforcement is on.
- Tables last committed by a Polaris version older than 1.3.0 have all their locations validated on
  their next commit. That commit fails if the locations do not satisfy the current location rules,
  for example the catalog's allowed locations.
- Loading a table whose metadata location or `write.data.path` / `write.metadata.path` differs from
  the values recorded in Polaris now fails with a `ForbiddenException`, without any commit being
  attempted. Tables last committed before 1.3.0 have no recorded values and are not affected until
  their next commit.
- Purging a table, and deleting metadata files after a commit
  (`write.metadata.delete-after-commit.enabled`), no longer delete files located outside the
  table's location. Such files are left in place and a warning is logged.
- Purging a table now fails with a 400 error if the table entity has no recorded location, and with
  a 403 error if the locations in the table metadata don't match the ones recorded on the entity.
  For tables created before 1.3.0, custom `write.data.path` and `write.metadata.path` locations are
  taken from the table metadata, since they were not recorded on the entity. Purge tasks that were
  queued by an older version only carry the table's base location: files under a custom
  `write.data.path` or `write.metadata.path` outside of it are not deleted by those tasks.

### Breaking changes

- Creating a Generic Table with an explicit, non-empty `base-location` now validates that
  location against the catalog's allowed locations and, unless `ALLOW_UNSTRUCTURED_TABLE_LOCATION`
  is enabled, the parent namespace location. Requests that previously stored disallowed locations
  are now rejected. Omitting the location or sending an empty string retains its previous behavior.
- `LIST_PAGINATION_ENABLED` now defaults to true. List APIs honor pagination parameters and reject
  invalid values. Clients must follow next-page-token to retrieve all results when requesting a page
  size or supplying a page token. A request that supplies neither still returns all results, unless
  a positive `LIST_PAGINATION_MAX_PAGE_SIZE` is configured and the result does not fit, in which
  case it is rejected rather than truncated. To keep the previous behavior, set
  `LIST_PAGINATION_ENABLED=false` or the catalog property `polaris.config.list-pagination-enabled=false`.
- The `PolarisPrincipal` interface has evolved. The `getAttributes()` method now returns 
  `org.apache.polaris.core.collection.ImmutableAttributeMap`. The attribute keys were moved to a
  new `org.apache.polaris.core.auth.PolarisPrincipalAttributes` class.
- Relational JDBC: Per-version schema scripts (`schema-v1.sql` through `schema-v5.sql`) have been
  replaced by a single `schema.sql` that is safe to run on every startup. Per-version runtime
  compatibility fallbacks and the `SCHEMA_VERSION_FALL_BACK_ON_DNE` configuration key have been
  removed. Operators must ensure their database is at the right schema version before upgrading to 
  this version.
- `PolarisAuthorizer` implementations should resolve entities with respect to selectors recorded
  in `AuthorizationState`. Implementations that used to call `PolarisResolutionManifest.resolveAll()`
  are functionally compatible with the new Polaris code. However, implementations that used to call
  `resolveSelections()` may be incompatible with the new Polaris code, because in some cases the
  REST API implementation code requests additional selections via `AuthorizationState`.
  All implementations should call `AuthorizationState.resolve()` now.
- The event attribute `EventAttributes.ACCESS_DELEGATION_MODE` (`String`) has been replaced with
  `EventAttributes.ACCESS_DELEGATION_MODES` (`List<String>`). Accordingly, the OpenTelemetry event
  listener now emits `polaris.access_delegation_modes` instead of `polaris.access_delegation_mode`.
- Table and view renames now validate locations against the destination namespace. Existing tables
  also validate their namespace location during commits and credential vending. With
  `ALLOW_UNSTRUCTURED_TABLE_LOCATION=false` (default), operations involving locations outside the
  namespace now return HTTP 403. Catalogs that enable unstructured locations remain subject to
  catalog allowlists and configured overlap checks.
- With `ALLOW_UNSTRUCTURED_TABLE_LOCATION=false`, creating or registering a table whose
  `write.data.path` or `write.metadata.path` lies outside the table's own location now fails with
  403 Forbidden. Such tables could be created before, but loading them or vending credentials for
  them already failed.
- Unless `ALLOW_TABLE_LOCATION_OVERLAP` is enabled, a table can no longer be created at a location
  that overlaps the `write.data.path` or `write.metadata.path` of an existing sibling table, and
  sibling tables can no longer share such a path. A namespace can no longer be created at such a
  location unless `ALLOW_NAMESPACE_LOCATION_OVERLAP` is enabled.
- With `OPTIMIZED_SIBLING_CHECK` enabled, Polaris now also lists siblings after the indexed
  overlap check when `ALLOW_UNSTRUCTURED_TABLE_LOCATION` is enabled or the catalog is external, to
  check sibling tables' `write.data.path` and `write.metadata.path`. Validation may take longer in
  such catalogs for namespaces with many entities. Checks against other tables' write paths remain
  limited to siblings under the same parent.
- With `ALLOW_UNSTRUCTURED_TABLE_LOCATION=true`, and in `EXTERNAL` catalogs, credentials are no
  longer vended for a table whose `write.data.path` or `write.metadata.path` lies outside its own
  location and overlaps another table or namespace. Such requests fail with 403 Forbidden. This
  check does not apply when `ALLOW_TABLE_LOCATION_OVERLAP` is enabled, and it only applies to
  catalogs served by `LocalIcebergCatalog`, not to federated catalogs.

### New Features

- The `X-Iceberg-Access-Delegation` header now accepts `none`, as defined by the Iceberg REST
  specification, meaning the client can operate without delegated access. A request that includes
  `none`, alone or together with other values, is handled like a request without the header: the
  table is returned without credentials and no delegation privilege is required.
- Shared CloudEvents 1.0 JSON mapper for Polaris events (`CloudEventsEventJsonMapper` next to
  the events SPI): CE context attributes at the top, Kafka-parity Polaris fields in `data`, and a
  lowercased reverse-DNS `type` (e.g. `org.apache.polaris.after_create_table`). Transport listeners
  (webhook / Kafka / CloudWatch) can adopt this in follow-ups.
- Python CLI: added `register` support for both `tables` and `views` commands
- Python CLI: `setup export` and `setup apply` now support both Iceberg tables and views as register entries.
- Python CLI: added `namespaces update` to set and remove namespace properties
- NoSQL: the commit retry settings are now configurable via `polaris.persistence.retry.*`
- Python CLI: added global `--ssl-ca-cert` and `--insecure` options to verify the server with a custom CA bundle or to disable TLS certificate verification

### Changes

- Azure credential vending now maps MSAL authentication failures using their HTTP status codes,
  including the existing `401` to `403` and `404` to `400` mappings.
- `PolarisMetaStoreManager.hasOverlappingSiblings` and `BasePersistence.hasOverlappingSiblings` now
  take the ids of the entity's catalog and parent namespaces, so implementations exclude them
  without re-reading the parent chain from the metastore, and return a `LocationOverlapResult`. They
  also take an optional search root: when set, only the entities located at or below it are
  considered, which allows renamed entities to be checked against the destination namespace only.
  Implementations must also ignore the entity itself, identified by its id, so that an already
  persisted entity does not conflict with its own location.
- Apache Iceberg has been upgraded to 1.12.0. The Java REST client now encodes spaces in namespace
  and table names as `%20` instead of `+` (apache/iceberg#15989). Polaris decodes path segments
  per RFC 3986 and has always treated `+` as a literal character, so the server behaves the same for
  all clients.
- The entity cache now also weighs the grant records of cached entities, so `ENTITY_CACHE_WEIGHER_TARGET`
  accounts for them. Entities with many grants weigh more, which can lead to more evictions than before
  with the same configured value. The description of `ENTITY_CACHE_WEIGHER_TARGET` now documents that
  its weights approximate bytes.
- The `apache-polaris` Python package now accepts `pydantic` from 2.12 up to, but excluding, 3.0
  instead of a single minor version, so it can be installed alongside packages that pin `pydantic`.
- `TABLE_MANAGE_STRUCTURE` and the fine-grained table update privileges (such as
  `TABLE_ASSIGN_UUID`, `TABLE_ADD_SCHEMA` or `TABLE_SET_PROPERTIES`) now subsume
  `TABLE_READ_PROPERTIES`, consistent with `TABLE_WRITE_PROPERTIES`, because a table update returns
  the full table metadata. Roles holding only one of these privileges can now also load the table,
  get the applicable policies on the table, and load the table as a generic table, since all of these
  operations require `TABLE_READ_PROPERTIES`.
  In the Ranger service definition, the corresponding access types (`table-structure-manage`,
  `table-uuid-assign`, etc.) now imply `table-properties-read`; the service definition must be
  re-imported into Ranger for this to take effect.

### Fixes

- Credential vending no longer ends up with no allowed read/write locations when a table's data
  or metadata path matches its base location except for the scheme (for example `abfss://` vs
  `abfs://`) or a trailing slash. Such locations are mutual children, so the previous logic dropped
  both and left the set empty. The base location is now kept as the surviving representative.
- The Iceberg REST `/config` response now advertises the
  `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials` endpoint, which Polaris
  already served. Clients that check advertised endpoints before refreshing vended credentials can
  now use it.
- Creating an external catalog whose `connectionConfigInfo` has no `authenticationParameters`, or
  uses a connection or authentication type outside the realm's allow-list, now fails with
  `400 Bad Request` instead of `500 Internal Server Error`.
- Iceberg REST `GET /v1/config` can optionally require `CATALOG_READ_CONFIG` (operation
  `GET_CATALOG_CONFIG`). The check is off by default behind
  `ENFORCE_CATALOG_CONFIG_AUTHORIZATION`; when enabled, unauthorized callers get 403. A 200 still
  returns catalog properties (`defaults`) as today. Previously any authenticated principal who
  knew a warehouse name could call the endpoint with no AuthZ.
- Helm chart: `helm test` now runs the chart's connection test. Previously the test was left out of
  the packaged chart, so `helm test` ran no tests.
- Credential vending with a statically configured GCP access token (`polaris.storage.gcp.token`)
  failed immediately with an `IllegalStateException`, because the token was wrapped in a credential
  that cannot refresh and vending calls refresh before downscoping. The configured token is now
  wrapped in a credential that supports refresh, returning the token with a fixed expiry. The expiry
  can be set explicitly via `polaris.storage.gcp.expiry` (an ISO-8601 instant); when unset it is
  computed from `polaris.storage.gcp.lifespan` as before.
- NoSQL commits resend unresolved object writes on retry, preventing successful commits from
  referencing objects left unwritten by a previous attempt.
- Maintenance policy content now rejects non-boolean values for the `enable` field again. The
  strict boolean check (`StrictBooleanDeserializer`) stopped running when policy parsing moved to
  Jackson 3, because it was wired through a Jackson 2 `@JsonDeserialize` annotation that the Jackson
  3 mapper ignores. As a result values such as `"TRUE"`, `1` and `0` were leniently coerced instead
  of rejected. The deserializer is ported to the Jackson 3 API and applied to the `enable` creator
  parameter, so only JSON `true`/`false` (and the lowercase string forms) are accepted.
- The H2 relational JDBC init script no longer overwrites the recorded schema version. It now seeds
  the `version` table only when absent, matching the `ON CONFLICT DO NOTHING` behavior of the
  PostgreSQL and CockroachDB scripts. Previously, running bootstrap against an older, unmigrated H2
  database silently stamped it with the current version, which defeated the schema-compatibility
  check and let the server run against an unmigrated schema.
- NoSQL maintenance: realm purging now honors its own state filter consistently. `purgeRealms`
  keeps only realms in `PURGING`/`PURGED` state (and logs the rest as "will therefore not be
  purged"), but the direct `backend.deleteRealms(...)` call and the subsequent realm-status update
  used the unfiltered run-spec list, so a realm the filter had excluded (for example an `ACTIVE`
  realm whose ID was listed in a stale spec) could still be wiped while the log claimed it was
  skipped, and the attempt to mark it `PURGED` failed the whole run on an invalid state transition.
  The direct deletion, the purged-realm count, and the status update now use the same filtered set
  as the scan-based deletion.
- Creating a table or view at exactly its namespace's own location is now rejected under the
  default (non-optimized) sibling-overlap check, matching the behavior already enforced when
  `OPTIMIZED_SIBLING_CHECK` is enabled. Such a location is a prefix of every sibling created later
  under the namespace, which blocked all subsequent sibling creates and over-scoped credentials
  vended for that entity to the whole namespace.
- Internal token verification now tolerates up to 5 seconds of clock skew on the `iat` claim. With
  multiple Polaris nodes, a token minted on one node and immediately used on another node whose
  clock was a few milliseconds behind could be rejected with HTTP 401 when the token was minted
  right after a second boundary. Token expiry is still enforced without leeway.
- NoSQL maintenance: spilled `IndexStripeObj` segments are retained when identify walks an index via
  `indexForRead` (`RetainedCollectorImpl.buildReadIndex`), and `PrincipalsObj.byClientId` is retained
  explicitly. Previously only a subset of indexes got `indexRetain`, so after a spill past
  `maxEmbeddedIndexSize` maintenance could delete still-needed stripes and leave catalog indexes
  unreadable (`references a reference index, which does not exist`).
- The `root` principal can no longer be dropped or renamed. Realm bootstrap identifies a
  bootstrapped realm by the presence of a principal named `root`, so removing or renaming it caused
  every newly started process to treat the realm as not bootstrapped (failing all requests,
  including the token endpoint), and re-running bootstrap could not cleanly repair it. It is now
  protected like the `service_admin` and `catalog_admin` roles.
- Dropped-table purge on NoSQL no longer fails when a manifest or metadata path is longer than
  the 500-byte index-key limit. Child cleanup task names are a short kind, the parent task id,
  and a UUID. The full paths stay in the task payload.
- Deleting an external catalog now also deletes the inline connection secrets (OAuth client secret
  or bearer token) that were written to the `UserSecretsManager` when the catalog was created.
  Previously they stayed in the secrets store with no entity referencing them.
- Catalog federation: `connectionConfigInfo.properties` is now persisted and returned for `HADOOP`
  and `HIVE` connection configurations. It was previously accepted by the management API but
  silently dropped, so `GET /catalogs/{name}` never showed it.
- Catalog federation: `connectionConfigInfo.properties` is now applied when Polaris initializes
  the Iceberg client for `HADOOP` and `HIVE` catalogs. As for `ICEBERG_REST`, only an explicit
  allow-list is forwarded: `suppress-permission-error` (Hadoop), `clients` and `list-all-tables`
  (Hive), plus the non-secret FileIO settings `s3.endpoint`,
  `s3.path-style-access` and `client.region` for both. Other keys are stored but ignored.
- Semantic models: a `datasets` field that is not a JSON array now returns `400 Bad Request`
  instead of being silently skipped, which bypassed every `dataset.source` check.
- Conditional `loadTable` (`If-None-Match` → HTTP 304) no longer attaches a null
  `LOAD_TABLE_RESPONSE` to the `AFTER_LOAD_TABLE` event. The persistence event listener also
  skips null attribute values instead of failing while pruning them.
- NoSQL index keys: `IndexKey.skip` now counts decoded key length the same way as
  `deserializeKey`. Previously it counted two units per escaped `0x01`/`0x02` byte against the
  same 500 limit, so a key that had been written successfully (for example 251 bytes of `0x01`)
  could make index deserialization throw and leave the index container unreadable.
- The credential-rotation pre-condition is now enforced by a JAX-RS filter instead of by each
  authorizer. With `ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING` enabled, a principal
  whose credentials were reset but never rotated can only call the rotate-credentials endpoint,
  whichever authorizer is configured. Previously the internal and Ranger authorizers enforced it
  but OPA did not, and the rotation state is not part of the OPA input document, so a policy could
  not compensate.
- Re-creating an existing namespace now returns `409 Conflict` instead of `403 Forbidden` when
  `OPTIMIZED_SIBLING_CHECK` is on. Namespace creation checks for an existing namespace before
  validating locations, as table and view creation already do, so the existing namespace's own
  location is no longer reported as a conflict.
- A list request whose `pageSize` is not a number now returns `400 Bad Request` naming the
  parameter, instead of `404 Not Found`. The status is now the same on every API that accepts
  `pageSize`.
- Ranger authorizer: a table or policy under a nested namespace is no longer mapped to the wrong
  Ranger resource. Namespace levels now occupy a single namespace resource instead of one each,
  so a policy written for the nested namespace matches.
- Updating a policy without `current-policy-version` now returns `400 Bad Request` naming the
  missing field, instead of `500 Internal Server Error`. The field is used for the policy's
  optimistic version check, so a request without it is rejected and the policy is unchanged.
- Ranger authorizer: `table-data-write` now confers `table-properties-write`, as the built-in
  authorizer already does. Without it, a Ranger policy granting only `table-data-write` was
  refused operations the same grant allows under the built-in authorizer. Existing deployments
  must re-register the updated service definition with Ranger Admin for this to take effect;
  upgrading Polaris alone does not change the definition already persisted there.
- Policy API: detaching a policy from a target it was never attached to now returns
  `404 Not Found` with error type `NoSuchMappingException`, as the policy API specification
  requires, instead of `500 Internal Server Error`.
- One failing event no longer fails an entire OpenLineage batch ingest request. It is reported as a
  non-retriable failed entry for that event.
- Registering a table whose stored metadata file is no longer readable no longer fails: with
  overwrite it replaces the metadata location, and without overwrite it reports the table as
  already existing.
- Relational JDBC: the location-overlap check now escapes SQL `LIKE` wildcards (`%`, `_`) and the
  escape character (`\`) that appear literally in a table or namespace location. Previously these
  characters were interpreted as wildcards, which could over-fetch candidate rows and, for a literal
  `\`, silently miss true descendants during the overlapping-siblings check.
- OPA authorizer HTTP client creation no longer silently falls back to a default client when
  truststore or SSL setup fails. Misconfiguration (for example a bad truststore path) now fails
  startup instead of continuing with system trust and no configured response timeout.
- `bootstrap` no longer creates realms whose root principal is unreachable. Credentials were
  required only when none at all were supplied, so an invocation naming credentials for just
  some of its realms bootstrapped the rest with randomly generated secrets that were never
  printed, and reported them successfully bootstrapped. Every `--realm` must now have a
  matching `--credential` unless `--print-credentials` is given; otherwise the command names
  the realms that are missing credentials and exits without bootstrapping anything.
  `--credentials-file` is unaffected.
- Table notifications (`CREATE`/`UPDATE`) that reference a metadata location outside the catalog's
  allowed locations are now rejected before any missing parent namespaces are auto-created, so a
  rejected notification no longer leaves orphaned namespaces behind.
- Fixed `OPTIMIZED_SIBLING_CHECK` rejecting every entity created under a namespace. The location
  index lookup returns the new entity's own parent namespaces, whose locations contain the new
  location whenever locations follow the namespace tree, and each backend treated them as
  overlapping siblings, so nested namespace creation and default-location table creation failed
  with `403 Forbidden`. The JDBC, NoSQL, and in-memory implementations of `hasOverlappingSiblings`
  now exclude the entity's ancestors (when they strictly contain it) before reporting an overlap,
  matching the legacy sibling check.
- Semantic-model create and update requests now require `semantic_model` JSON to be an object and
  validate every dataset source. Previously, invalid root shapes could bypass source validation or
  be accepted under the obsolete array contract.
- Python client deserialization and CLI `setup export` now preserve the remote catalog name and
  warehouse for Iceberg REST, Hadoop, and Hive external catalogs.
- Honored pagination for generic table API.
- JDBC optimized location-overlap queries no longer include the lone `/` prefix term produced by
  scheme stripping (e.g. `s3://bucket/path` → `//bucket/path`). `//` and `///` are retained so
  scheme-root ancestors remain visible to the overlap check.
- Honored pagination for policy API with applicable-policies endpoint excluded.
- The NoSQL metastore now honors `ALLOW_DROPPING_NON_EMPTY_PASSTHROUGH_FACADE_CATALOG`: dropping a
  non-empty passthrough-facade (federated) catalog previously always failed with NoSQL persistence,
  even when the flag was enabled.
- The refresh-credentials endpoint advertised by `loadTable` and `loadCredentials`, and the
  generic-table paths used by the Spark plugin, now encode spaces in namespace and table names as
  `%20` instead of `+`. Previously, credential refresh failed for tables whose namespace or name
  contained a space.
- NoSQL: a required credential rotation now invalidates the previous secret. The stored
  credential-rotation state was not considered when deciding whether to drop the previous secret,
  so the secret issued before the rotation stayed usable as the principal's secondary secret,
  unlike on the relational-JDBC and transactional backends.
- The MongoDB readiness check is now shown in `/q/health` only when the NoSQL MongoDB backend is
  selected. Other persistence backends no longer show the unused check. MongoDB deployments keep
  readiness monitoring without configuration changes.
- Fixed an unbounded memory growth in the in-memory entity cache that could lead to out-of-memory
  errors: lookups by name no longer hold entities outside the cache's weight and expiry limits.
- Setting a namespace's `location` property no longer fails with an internal error when its parent
  is itself a nested namespace that has no stored location.
- The Ranger authorizer no longer fails with `NoClassDefFoundError: javax/ws/rs/core/Cookie` when
  using `RangerAdminRESTClient` as the policy source. Ranger 2.9 no longer bundles the JAX-RS 1.x
  API, so `javax.ws.rs:jsr311-api` is now shipped with the Ranger extension (see #5728).
- Iceberg REST: creating a namespace with a request that omits `namespace` now returns
  `400 Bad Request` instead of `500 Internal Server Error`.

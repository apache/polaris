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

### Upgrade Notes

- Amazon RDS plugin enabled, this allows polaris to connect to AWS Aurora PostgreSQL using IAM authentication.
- The EclipseLink Persistence implementation has been deprecated since 1.0.0 and will be completely removed
  in 1.3.0 or in 2.0.0 (whichever happens earlier).

### Breaking Changes

### New Features

- Added a Management API endpoint to reset principal credentials, controlled by the `ENABLE_CREDENTIAL_RESET` (default: true) feature flag.
- The `ENABLE_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS` was added to support sub-catalog (initially namespace and table) RBAC for federated catalogs.
  The setting can be configured on a per-catalog basis by setting the catalog property: `polaris.config.enable-sub-catalog-rbac-for-federated-catalogs`.
  The realm-level feature flag `ALLOW_SETTING_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS` (default: true) controls whether this functionality can be enabled or modified at the catalog level.

- Added support for S3-compatible storage that does not have STS (use `stsUavailable: true` in catalog storage configuration)

### Changes

* The following APIs will now return the newly-created objects as part of the successful 201 response: createCatalog, createPrincipalRole, createCatalogRole. 

### Deprecations

* The property `polaris.active-roles-provider.type` is deprecated and has no effect anymore.

### Fixes

* Fixed incorrect Azure expires at field for the credentials refresh response, leading to client failure via #2633

### Commits

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

[Unreleased]: https://github.com/apache/polaris/commits
[1.0.1-incubating]: https://github.com/apache/polaris/releases/tag/apache-polaris-1.0.1-incubating
[1.0.0-incubating]: https://github.com/apache/polaris/releases/tag/apache-polaris-1.0.0-incubating-rc2
[0.9.0-incubating]: https://github.com/apache/polaris/releases/tag/apache-polaris-0.9.0-incubating
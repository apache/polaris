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

- **HMS Federation Support**: Added support for Hive Metastore (HMS) federation, enabling integration with existing Hive metastores.

- **Modularized Federation**: Introduced modularized federation architecture to support multiple catalog types and improve extensibility.

- **External Authentication**: Added comprehensive support for external identity providers including Keycloak integration and Helm chart configuration options.

- **Python Client Distribution**: The Python client is now packaged and distributed as a proper Python package for easier installation and usage.

- **Catalog Federation CLI**: Extended the CLI with support for managing federated catalogs, making it easier to configure and operate catalog federation.

- **MinIO**: Added MinIO integration support with comprehensive getting started documentation.

### Upgrade notes

### Breaking changes

- Helm chart: the default value of the `authentication.tokenBroker.secret.symmetricKey.secretKey` property has changed
  from `symmetric.pem` to `symmetric.key`.

### New Features

- Added Catalog configuration for S3 and STS endpoints. This also allows using non-AWS S3 implementations.
  The realm-level feature flag `ALLOW_SETTING_S3_ENDPOINTS` (default: true) may be used to disable this
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

### Changes

- Polaris Management API clients must be prepared to deal with new attributes in `AwsStorageConfigInfo` objects.

- S3 configuration property role-ARN is no longer mandatory.

### Deprecations

- The property `polaris.active-roles-provider.type` is deprecated for removal.
- The `ActiveRolesProvider` interface is deprecated for removal.

### Fixes

### Commits

## [1.0.0-incubating]

- TODO: backfill 1.0.0 release notes

[Unreleased]: https://github.com/apache/polaris/commits
[1.0.0-incubating]: https://github.com/apache/polaris/releases/tag/apache-polaris-1.0.0-incubating-rc2

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

### Breaking changes

### New Features

- Added Catalog configuration for S3 and STS endpoints. This also allows using non-AWS S3 implementations.

- The `IMPLICIT` authentication type enables users to create federated catalogs without explicitly
providing authentication parameters to Polaris. When the authentication type is set to `IMPLICIT`, 
the authentication parameters are picked from the environment or configuration files. 

- The `DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED` feature was added to support placing tables
at locations that better optimize for object storage.

### Changes

- Polaris Management API clients must be prepared to deal with new attributes in `AwsStorageConfigInfo` objects.

### Deprecations

* The property `polaris.active-roles-provider.type` is deprecated in favor of
  `polaris.authentication.active-roles-provider.type`. The old property is still supported, but will be removed in a
  future release.

### Fixes

### Commits

## [1.0.0-incubating]

- TODO: backfill 1.0.0 release notes

[Unreleased]: https://github.com/apache/polaris/commits
[1.0.0-incubating]: https://github.com/apache/polaris/releases/tag/apache-polaris-1.0.0-incubating-rc2

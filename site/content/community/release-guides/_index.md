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
linkTitle: Release Guides
title: Apache Polaris Release Guides
type: docs
weight: 500
cascade:
  type: docs
---

This section contains documentation related to Apache Polaris releases.

If you're new to releasing Apache Polaris, we recommend reading the manual release guide first to understand the process. Do not perform any actions described in the manual guide, as the semi-automated guide will handle them for you. Simply read through the manual guide to understand the process.

Once you are familiar with the manual release process, use the semi-automated guide to cut the release.

If you want to verify a release, head to the release verification guide.

## Available Release Guides

### Semi-Automated Release Guide
The [semi-automated release guide](semi-automated-release-guide/) describes how GitHub workflows are used to perform a release with little manual intervention. This guide automates many of the manual steps while maintaining the necessary oversight and validation required for Apache releases. It is the preferred approach for cutting Apache Polaris releases.

### Manual Release Guide (deprecated)
The [manual release guide](manual-release-guide/) walks through each step of creating an Apache Polaris release. This guide provides detailed instructions for every aspect of the release process but requires significant manual intervention. It is deprecated in favor of the semi-automated approach above, but is maintained for reference and fallback scenarios.

### Release Verification Guide
The [release verification guide](release-verification-guide/) provides instructions for verifying an Apache Polaris release, like verifying checksums and signatures, ensuring release artifacts integrity, ...

### Polaris Tools Releqse Guide
The [Polaris Tools manual release guide](tools-manual-release-guide/) provides instructions to release one of the Polaris Tools atomatically. For now, this release guide is manual (waiting the automatic release for Polaris Tools).

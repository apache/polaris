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
title: Apache Polaris Releases
linkTitle: Releases
weight: 200
cascade:
  type: docs
menus:
  main:
    parent: releases
    weight: 1
    identifier: downloads-overview
    name: Overview
---

This section lists source and binary releases for Apache Polaris. For each release, it provides a link to the release notes and the download links for the release artifacts.

{{< alert tip >}}
The Apache Polaris Helm Chart can be found in the [Helm Chart Repository](https://downloads.apache.org/polaris/helm-chart).
{{< /alert >}}

### Active Releases

- [1.3.0](1.3.0/) - Released January 16th, 2026
- [1.2.0](1.2.0/) - Released October 23rd, 2025
- [1.1.0](1.1.0/) - Released September 19th, 2025

### End of Life Releases

- [1.0.1](1.0.1/) - Released August 16th, 2025
- [1.0.0](1.0.0/) - Released July 9th, 2025
- [0.9.0](0.9.0/) - Released March 11, 2025

## Verifying Downloads

All downloads can be verified using the Apache Polaris [KEYS](https://downloads.apache.org/polaris/KEYS) file.

### Verifying Signatures

First, import the keys:

```bash
curl https://downloads.apache.org/polaris/KEYS -o KEYS
gpg --import KEYS
```

Then verify the `.asc` signature files:

```bash
gpg --verify apache-polaris-[version].tar.gz.asc
```

### Verifying Checksums

```bash
shasum -a 512 --check apache-polaris-[version].tar.gz.sha512
```

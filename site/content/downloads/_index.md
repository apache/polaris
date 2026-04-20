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
  type: downloads
menus:
  main:
    parent: releases
    weight: -999999 # 1st item in the menu
    identifier: releases-overview
    name: Overview
---

This section provides access to Apache Polaris source and binary artifacts and their release notes.

{{< alert tip >}}
The Apache Polaris Docker images are available on [Docker Hub](https://hub.docker.com/r/apache/polaris). The Apache Polaris Helm Chart can be found in the [Helm Chart Repository](https://downloads.apache.org/polaris/helm-chart) or in [Artifact Hub](https://artifacthub.io/packages/helm/apache-polaris/polaris).
{{< /alert >}}

Select a version below to get started:

{{< downloads-list >}}

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

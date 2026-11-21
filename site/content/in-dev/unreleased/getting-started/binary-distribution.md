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
Title: Binary Distribution
type: docs
weight: 102
---
Use this guide to quickly start running Polaris using the pre-built binary distribution.

## Prerequisites

- Java SE 21 or later installed on your machine

## Running

Download the [latest release](https://polaris.apache.org/downloads/latest/) and extract the binary distribution:

```bash
POLARIS_VERSION=1.5.0
curl -Lf https://downloads.apache.org/polaris/${POLARIS_VERSION}/polaris-bin-${POLARIS_VERSION}.tgz | tar xzf -
cd polaris-bin-${POLARIS_VERSION}
```

Start the Polaris server:

```bash
bin/server
```

The server will start and listen on http://localhost:8181 (main REST APIs) and http://localhost:8182 (management interface, for health checks and metrics). 

Health and metrics endpoints are available under http://localhost:8182/q/health and http://localhost:8182/q/metrics respectively.

You can verify the server is running by checking the health endpoint:

```bash
curl http://localhost:8182/q/health
```

## Software bill of materials

Binary ZIP and TAR releases include a signed CycloneDX 1.6 JSON sidecar named
`polaris-bin-V.zip.cdx.json` or `polaris-bin-V.tgz.cdx.json`. Verify its `.asc`
signature using a trusted Polaris release key, then compare the SHA-256 in
`metadata.component.hashes` with the downloaded archive. Each document describes
both packaged applications and the other archive files; dependency completeness
is marked unknown.

Spark bundle JARs have separate SBOMs attached to their Maven publications with
classifier `bundle-sbom-cyclonedx`, also available with release assets.

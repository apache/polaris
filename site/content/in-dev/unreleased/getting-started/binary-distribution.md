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

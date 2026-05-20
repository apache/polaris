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
linkTitle: "Quickstart"
title: "Quickstart"
weight: 10
cascade:
    type: guides
menus:
    main:
        parent: Guides
        weight: 2
---

<!--
```shell
cd "$SITE_TEST_GUIDE_DIR"
docker compose up
```
-->

Welcome to Apache Polaris! This guide helps you quickly get Polaris running locally for evaluation and development purposes.  
**This setup is not intended for production use.**

## Prerequisites

- Docker and Docker Compose v2 installed and running.

## Running the Quickstart

Run the following command:

```bash
curl -s https://raw.githubusercontent.com/apache/polaris/refs/heads/main/site/content/guides/quickstart/docker-compose.yml | docker compose -p polaris-quickstart -f - up -d
```

This command will start Polaris + RustFS and automatically create:

- Catalog: `quickstart_catalog`
- Principal: `quickstart_user` (with full access)

Check the logs for credentials and example commands:

```bash
docker compose -p polaris-quickstart logs
```

### Useful URLs

- Polaris REST API: [http://localhost:8181](http://localhost:8181)
- RustFS Console: [http://localhost:9001](http://localhost:9001)

To stop the services:

```bash
docker compose -p polaris-quickstart down
```

## Next Steps

- [Binary Distribution](/releases/latest/getting-started/binary-distribution)
- [Setup Tools](/releases/latest/getting-started/setup-tools)
- [Spark Integration](/guides/spark)
- [Full Documentation](/releases/latest/)

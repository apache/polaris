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
Title: Quickstart
type: docs
weight: 99
---
Use this guide to quickly start running Polaris. This is not intended for production use.

## Prerequisites

- Have Docker (with Docker Compose v2) installed & running on your machine

## Running

Run the following command:

```bash
curl -s https://raw.githubusercontent.com/apache/polaris/main/getting-started/quickstart/docker-compose.yml | docker compose -f - up

```
This command will:
1. Create a Catalog named `quickstart_catalog` with MinIO-backed storage.
2. Create a user principal `quickstart_user` with full access to the catalog.

Once the command has been run, you will see examples on how to interact with this Polaris server in the logs.

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
title: storage-azure
build:
  list: never
  render: never
---

| Property | Type | Kind | Description |
|----------|------|------|-------------|
| `adls.sas-token.<account-host>` | `String` | credential | SAS token keyed by the full storage DNS name (e.g. `adls.sas-token.myaccount.dfs.core.windows.net`); consumed by Spark via Iceberg's ADLSFileIO |
| `adls.sas-token.<account-name>` | `String` | credential | SAS token keyed by the storage account name only (e.g. `adls.sas-token.myaccount`); consumed by Iceberg 1.7.x clients |
| `adls.sas-token` | `String` | credential | Bare SAS token (no suffix); consumed by PyIceberg via adlfs/fsspec |
| `adls.account-name` | `String` | credential | Storage account name; consumed by PyIceberg via adlfs/fsspec |
| `adls.refresh-credentials-endpoint` | `String` | config | Catalog endpoint the client can call to refresh vended credentials before they expire |
| `adls.sas-token-expires-at-ms.<account-host>` | `Long` | credential | Expiration time of the SAS token keyed by the full storage DNS name, in milliseconds since the Unix epoch |


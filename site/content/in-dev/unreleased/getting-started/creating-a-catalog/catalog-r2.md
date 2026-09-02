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
title: Creating a catalog on Cloudflare R2
linkTitle: R2
type: docs
weight: 400
---

For the `polaris catalogs create` [command]({{% ref "../../command-line-interface#create" %}}) there are few `r2` only options

```text
--storage-type r2
--account-id  (Required for R2) The Cloudflare account id, 32 lowercase hex characters
--jurisdiction  (Only for R2) The data jurisdiction: eu, fedramp, or us
```

### example

```shell
CLIENT_ID=root \
CLIENT_SECRET=s3cr3t \
DEFAULT_BASE_LOCATION=s3://my-bucket/warehouse  \
ACCOUNT_ID=0123456789abcdef0123456789abcdef \
polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  catalogs \
  create \
  --storage-type r2 \
  --default-base-location ${DEFAULT_BASE_LOCATION} \
  --account-id ${ACCOUNT_ID} \
  my_r2_catalog
```

Before creating an R2 catalog, an administrator must configure a server-side parent API token
for Polaris to mint temporary credentials from; see
[Configuring Polaris for Production — Cloudflare R2]({{% ref "../../configuration/configuring-polaris-for-production/configuring-cloudflare-r2-cloud-storage-specific" %}}).
If the server holds named parent tokens (`polaris.storage.r2.<name>.*`), add
`--storage-name <name>` to bind the catalog to one of them; without it the catalog uses the
server's default token.

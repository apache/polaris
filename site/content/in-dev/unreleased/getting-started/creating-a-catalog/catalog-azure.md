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
title: Creating a catalog on Azure
linkTitle: Azure
type: docs
weight: 300
---

For the `polaris catalogs create` [command]({{% ref "../../command-line-interface#create" %}}) there are few `azure` only options

```text
--storage-type azure
--tenant-id  (Required for Azure) A tenant ID to use when connecting to Azure Storage
--multi-tenant-app-name  (Only for Azure) The app name to use when connecting to Azure Storage
--consent-url  (Only for Azure) A consent URL granting permissions for the Azure Storage location
```

### example

```shell
CLIENT_ID=root \
CLIENT_SECRET=s3cr3t \
DEFAULT_BASE_LOCATION=abfss://tenant123@blob.core.windows.net \
TENANT_ID=tenant123.onmicrosoft.com \
MULTI_TENANT_APP_NAME=myapp \
CONSENT_URL=https://myapp.com/consent
./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  catalogs \
  create \
  --storage-type azure \
  --tenant-id ${TENANT_ID} \
  --multi-tenant-app-name ${MULTI_TENANT_APP_NAME} \
  --consent-url ${CONSENT_URL} \
  --default-base-location ${DEFAULT_BASE_LOCATION} \
  my_azure_catalog
```
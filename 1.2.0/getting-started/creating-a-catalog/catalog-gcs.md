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
title: Creating a catalog on Google Cloud Storage (GCS)
linkTitle: GCS
type: docs
weight: 200
---

For the `polaris catalogs create` [command]({{% ref "../../command-line-interface#create" %}}) there are few `gcs` only options

```text
--storage-type gcs
--service-account  (Only for GCS) The service account to use when connecting to GCS
```

### example

```shell
CLIENT_ID=root \
CLIENT_SECRET=s3cr3t \
DEFAULT_BASE_LOCATION=gs://my-ml-bucket/predictions/  \
SERVICE_ACCOUNT=serviceAccount:my-service-account@my-project.iam.gserviceaccount.com \
./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  catalogs \
  create \
  --storage-type gcs \
  --service-account ${SERVICE_ACCOUNT} \
  --default-base-location ${DEFAULT_BASE_LOCATION} \
  my_gcs_catalog
```
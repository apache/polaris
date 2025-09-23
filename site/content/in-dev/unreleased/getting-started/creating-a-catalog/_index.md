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
title: Creating a Catalog
linkTitle: Creating a Catalog
type: docs
weight: 300
---

The following Object Storage providers can be configured as storage backends for your Polaris catalog:

- [S3 compatible object stores]({{< ref "s3.md" >}})
- [Google Cloud Storage]({{< ref "catalog-gcs.md" >}})
- [Azure Blob Storage]({{< ref "catalog-azure.md" >}})
- Local file system (By default for testing only)


## Create a catalog using polaris CLI

Check full list of options for the `polaris catalogs create` command [here]({{% ref "../../command-line-interface#create" %}})

### Example

```shell
CLIENT_ID=root \
CLIENT_SECRET=s3cr3t \
DEFAULT_BASE_LOCATION=s3://example-bucket/my_data \
ROLE_ARN=arn:aws:iam::111122223333:role/ExampleCorpRole \
./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  catalogs \
  create \
  --storage-type s3 \
  --default-base-location ${DEFAULT_BASE_LOCATION} \
  --role-arn ${ROLE_ARN} \
  my_catalog
```

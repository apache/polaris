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
title: Creating a catalog on AWS S3
linkTitle: AWS
type: docs
weight: 100
---


### example

```shell
CLIENT_ID=root \
CLIENT_SECRET=s3cr3t \
DEFAULT_BASE_LOCATION=s3://example-bucket/my_data \
ROLE_ARN=arn:aws:iam::111122223333:role/ExampleCorpRole \
REGION=us-west-2 \
EXTERNAL_ID=12345678901234567890 \
./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  catalogs \
  create \
  --storage-type s3 \
  --default-base-location ${DEFAULT_BASE_LOCATION} \
  --role-arn ${ROLE_ARN} \
  --region ${REGION} \
  --external-id ${EXTERNAL_ID} \
  my_aws_catalog
```
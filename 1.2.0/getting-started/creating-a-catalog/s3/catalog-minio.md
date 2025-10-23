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
title: Creating a catalog on MinIO
linkTitle: MinIO
type: docs
weight: 200
---

When creating a catalog based on MinIO storage it is important to configure the `endpoint` property to point
to your own MinIO cluster. If the `endpoint` property is not set, Polaris will attempt to contact AWS
storage services (which is certain to fail in this case).

Note: the region setting is not required by MinIO, but it is set in this example for the sake of
simplicity as it is usually required by the AWS SDK (used internally by Polaris). One can also 
set the `AWS_REGION` environment variable in the Polaris server process and avoid setting region
as a catalog property.

Note: the name `quickstart_catalog` from the example below is referenced in other Getting Started examples,
but of course, it can be any valid catalog name.

```shell
CLIENT_ID=root
CLIENT_SECRET=s3cr3t
DEFAULT_BASE_LOCATION=s3://example-bucket/my_data
REGION=us-west-2

./polaris \
  --client-id ${CLIENT_ID} \
  --client-secret ${CLIENT_SECRET} \
  catalogs \
  create \
  --storage-type s3 \
  --endpoint http://127.0.0.1:9100
  --default-base-location ${DEFAULT_BASE_LOCATION} \
  --region ${REGION} \
  quickstart_catalog
```

In more complex deployments it may be necessary to configure different endpoints for S3 requests
and for STS (AssumeRole) requests. This can be achieved via the `--sts-endpoint` CLI option.

Additionally, the `--endpoint-internal` CLI option cane be used to set the S3 endpoint for use by
the Polaris Server itself, if it needs to be different from the endpoint used by clients / engines.

A usable MinIO example for `docker-compose` is available in the Polaris source code under the
[getting-started/minio](https://github.com/apache/polaris/tree/main/getting-started/minio) module.

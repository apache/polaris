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
title: Creating a catalog on S3 compatible cloud providers
linkTitle: S3
type: docs
weight: 100
---

The following S3 compatible cloud providers can be configured as storage backends for your Polaris catalog:

- [AWS S3]({{< ref "catalog-aws.md" >}})
- [MinIO]({{< ref "catalog-minio.md" >}})

For the `polaris catalogs create` [command]({{% ref "../../../command-line-interface#create" %}}) there are few `s3` only options

```text
--storage-type s3
--role-arn  (Only for AWS S3) A role ARN to use when connecting to S3
--region  (Only for S3) The region to use when connecting to S3
--external-id  (Only for S3) The external ID to use when connecting to S3
```

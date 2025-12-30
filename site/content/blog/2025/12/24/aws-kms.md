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
title: "Securing S3 data with AWS KMS"
date: 2025-12-24
author: Dmitri Bourlatchkov
---
## Introduction

AWS [Key Management Service](https://docs.aws.amazon.com/kms/latest/developerguide/overview.html) (KMS) provides
a way to encrypt S3 data in AWS without exposing raw key material outside AWS services.

Apache Polaris supports using KMS in its catalogs backed by AWS S3 storage.

The core functionality is available via Polaris REST API since the `1.2.0-incubating` release.
CLI support will be made available in the release following `1.3.0-incubating`.

## Configuring Polaris Catalog

KMS settings in Polaris are relevant to S3 buckets that have been confugure to use KMS on the AWS side
(e.g. using SSE-KMS).

Make a note of the KMS keys ARN that the bucket uses and pass it to the `--current-kms-key` CLI option
when creating the corresponding Polaris Catalog.

For example:

```shell
./polaris \
  --client-id ${POLARIS_CLIENT_ID} \
  --client-secret ${POLARIS_CLIENT_SECRET} \
  catalogs \
  create \
  --storage-type s3 \
  --default-base-location ${S3_LOCATION_URI} \
  --role-arn ${ROLE_ARN} \
  --region ${REGION} \
  --external-id ${EXTERNAL_ID} \
  --current-kms-key ${KMS_ARN} \
  quickstart_catalog
```

Once the KMS key is configured in the catalog, Polaris will automatically add appropriate access
policy entries to vended credentials. Clients do not need to take any extra actions to benefit
from KMS-based server-side data encryption and decryption. This applies to engines (like Spark)
and to AWS clients used inside Polaris itself (for reading and writing metadata JSON files).

## Common Failure Modes

If KMS keys are associated with the S3 bucket, but not configured in Polaris, clients will face
runtime errors when communicating with S3 APIs. The following example is from Spark.

```
25/12/24 14:32:20 ERROR SparkSQLDriver: Failed in [select * from ns.t1]
software.amazon.awssdk.services.s3.model.S3Exception: User: arn:aws:sts::123456789012:assumed-role/polaris/PolarisAwsCredentialsStorageIntegration is not authorized to perform: kms:Decrypt on resource: arn:aws:kms:us-west-2:123456789012:key/abcd1234-1111-2222-3333-444444444444 because no session policy allows the kms:Decrypt action (Service: S3, Status Code: 403, Request ID: ****************, Extended Request ID: *****)
```

```
spark-sql ()> insert into ns.t1 values ('test');
[...]
25/12/24 14:24:49 ERROR AppendDataExec: Data source write support IcebergBatchWrite(table=polaris.ns.t1, format=PARQUET) aborted.
Job aborted due to stage failure: Task 0 in stage 3.0 failed 1 times, most recent failure: Lost task 0.0 in stage 3.0 (TID 3) (192.168.68.56 executor driver): java.io.UncheckedIOException: Failed to close current writer
[...]
Caused by: java.io.IOException: software.amazon.awssdk.services.s3.model.S3Exception: User: arn:aws:sts::123456789012:assumed-role/polaris/PolarisAwsCredentialsStorageIntegration is not authorized to perform: kms:GenerateDataKey on resource: arn:aws:kms:us-west-2:123456789012:key/abcd1234-1111-2222-3333-444444444444 because no session policy allows the kms:GenerateDataKey action (Service: S3, Status Code: 403, Request ID: ****************, Extended Request ID: ************)
```

## Using Multiple KMS Keys

If the bucket used by the catalog has had multiple different KMS key ARNs associated with it over time,
Polaris needs to know all related key ARNs. This is necessary for the catalog server to properly form policies
associated with vended credentials so that accessing both old and new data is possible.

This can be achieved by using the `--allowed-kms-key` CLI option to add zero or more extra KMS key ARNs to the
catalog's storage configuration.

Note: the key material may be automatically rotated by AWS services (if configured) without introducing a new key ARN,
in that case no catalog changes are necessary.

## Acknowledgements

KMS support in Polaris was made possible through collaboration with many community members, specifically involving PRs
#[1424](https://github.com/apache/polaris/pull/1424) by [fivetran-ashokborra](https://github.com/fivetran-ashokborra) 
and #[2802](https://github.com/apache/polaris/pull/2802) by [fabio-rizzo-01](https://github.com/fabio-rizzo-01). 

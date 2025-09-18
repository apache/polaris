<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
 
   http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Polaris Runtime Service

## Overview
The `polaris-runtime-service` module is the core runtime service implementation of Apache Polaris. It serves as the main service layer that provides the REST API endpoints and business logic.

## Integration tests

Integration tests from the :polaris-tests module can be run against a local Polaris Quarkus instance
for each supported cloud storage. Set the appropriate environment variables for your target cloud,
then run the tests as shown below.

For S3:
```shell
export INTEGRATION_TEST_S3_PATH="s3://bucket/subpath"
export INTEGRATION_TEST_S3_ROLE_ARN="your-role-arn"
./gradlew :polaris-runtime-service:cloudTest
```
For Azure:
```shell
export INTEGRATION_TEST_AZURE_PATH="abfss://bucket/subpath"
export INTEGRATION_TEST_AZURE_TENANT_ID="your-tenant-id"
./gradlew :polaris-runtime-service:cloudTest
``` 
For GCS:
```shell
export INTEGRATION_TEST_GCS_PATH="gs://bucket/subpath"
export INTEGRATION_TEST_GCS_SERVICE_ACCOUNT="your-service-account"
./gradlew :polaris-runtime-service:cloudTest
```

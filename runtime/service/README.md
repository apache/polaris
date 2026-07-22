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
For ADLS:
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
To run the GCP federated catalog cloud tests using externally supplied end-user credentials:
```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/application-default-credentials.json
export INTEGRATION_TEST_GCS_FEDERATED_CATALOG=YOUR_BIGLAKE_REST_CATALOG
export INTEGRATION_TEST_GCS_FEDERATED_PATH=gs://your-bucket/your-prefix
export INTEGRATION_TEST_GCS_FEDERATED_QUOTA_PROJECT=YOUR_BILLING_PROJECT
export INTEGRATION_TEST_GCS_SERVICE_ACCOUNT=YOUR_SERVICE_ACCOUNT@YOUR_PROJECT_ID.iam.gserviceaccount.com
./gradlew :polaris-runtime-service:cloudTest --tests "org.apache.polaris.service.it.GcpCatalogFederationIntegrationIT"
```

Optional environment variables for the BigLake cloud tests:
```shell
export INTEGRATION_TEST_GCS_FEDERATED_PRESERVE_RESOURCES=true
export INTEGRATION_TEST_GCS_FEDERATED_REFRESH_WAIT_SECONDS=3900
```

`INTEGRATION_TEST_GCS_FEDERATED_PRESERVE_RESOURCES=true` keeps the test-owned Polaris catalog,
namespaces, tables, and principals for troubleshooting instead of cleaning them up.

`INTEGRATION_TEST_GCS_FEDERATED_REFRESH_WAIT_SECONDS` enables the longer-running OAuth refresh
coverage. Set it to a positive number of seconds that exceeds the expected token lifetime for your
environment if you want the credential-refresh scenario to run; otherwise that scenario is skipped.

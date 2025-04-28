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

# Apache Polaris API Modules

This directory contains the API modules for Apache Polaris.

## Modules

- [`polaris-api-management-model`](management-model): contains the model classes for the Polaris
  Management API.
- [`polaris-api-management-service`](management-service): contains the service classes for the 
  Polaris Management API.
- [`polaris-api-iceberg-service`](iceberg-service): contains the service classes for the Polaris
  Iceberg REST API.
- [`polaris-api-catalog-service`](polaris-catalog-service): contains the service classes for the Polaris
  native Catalog REST API.

The classes in these modules are generated from the OpenAPI specification files in the
[`spec`](../spec) directory.
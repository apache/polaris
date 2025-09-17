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

# Apache Polaris API Specifications

Apache Polaris provides the following OpenAPI specifications:

- [polaris-management-service.yml](polaris-management-service.yml) - Defines the management APIs for using Apache
  Polaris to create and manage Apache Iceberg catalogs and their principals

- [polaris-catalog-service.yaml](polaris-catalog-service.yaml) - Defines the specification for the Apache Polaris 
  Catalog API, which encompasses both the Apache Iceberg REST Catalog API and Apache Polaris-native APIs:

  - [iceberg-rest-catalog-open-api.yaml](iceberg-rest-catalog-open-api.yaml) - Contains the specification for 
    Apache Iceberg Rest Catalog API.

  - [polaris-catalog-apis](polaris-catalog-apis) - This folder contains the specifications for Apache 
    Polaris-specific Catalog APIs:

    - [generic-tables-api.yaml](polaris-catalog-apis/generic-tables-api.yaml) - Contains the specification for 
      the Generic Tables API

    - [notifications-api.yaml](polaris-catalog-apis/notifications-api.yaml) - Contains the specification for 
      the Notifications API

    - [oauth-tokens-api.yaml](polaris-catalog-apis/oauth-tokens-api.yaml) - Contains the specification for the 
      internal OAuth Token endpoint, extracted from the Apache Iceberg REST Catalog API.

    - [policy-apis.yaml](polaris-catalog-apis/policy-apis.yaml) - Contains the specification for the Policy APIs.

- [s3-sign](s3-sign) - This folder contains the specifications for S3 remote signing:

  - [iceberg-s3-signer-open-api.yaml](s3-sign/iceberg-s3-signer-open-api.yaml) - Contains the specification of the 
    original Apache Iceberg S3 Signer API.

  - [polaris-s3-sign-service.yaml](s3-sign/polaris-s3-sign-service.yaml) - Contains the Apache Polaris-specific 
    S3 Signer API.

## Generated Specification Files
The specification files in the generated folder are automatically created using OpenAPI bundling tools such as 
[Redocly CLI](https://github.com/Redocly/redocly-cli).

These files should not be manually edited (except adding license header). They are intended for preview purposes only, 
such as rendering a preview on a website.

Whenever the source specification files are updated, the generated files must be re-generated to reflect those changes.

Below are steps to generate `bundled-polaris-catalog-service.yaml`
### Install redocly-cli
```
npm install @redocly/cli -g
```

### Generate the Bundle
```
redocly bundle spec/polaris-catalog-service.yaml -o spec/generated/bundled-polaris-catalog-service.yaml
```
Note: the license header will be removed after the bundle generation, please manually add it back.

### Updating the Iceberg specification

The file `iceberg-rest-catalog-open-api.yaml` is copied from the upstream Iceberg REST catalog spec.

However, when copying it, you may need to make some nonfunctional changes to ensure that the generated Python types
still allow all tests to pass. You can regenerate the Python client by running:
```
make client-regenerate
```
For more context, see [PR #2192](https://github.com/apache/polaris/pull/2192).


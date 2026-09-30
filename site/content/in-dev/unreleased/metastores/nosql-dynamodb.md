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
title: NoSQL DynamoDB
type: docs
weight: 400
---

{{< alert note >}}
The DynamoDB backend is experimental.
{{< /alert >}}

The DynamoDB backend stores Polaris metadata in Amazon DynamoDB. In a Quarkus deployment, Polaris
uses the client supplied by the Quarkus Amazon DynamoDB extension.

## Basic configuration

Select the NoSQL backend and configure the DynamoDB client region. Credential acquisition, endpoint
overrides, retry behavior, and HTTP settings use the standard Quarkus DynamoDB configuration:

```properties
polaris.persistence.type=nosql
polaris.persistence.nosql.backend=DynamoDb
quarkus.dynamodb.aws.region=us-east-1
```

For example, local development can configure static credentials and an endpoint override:

```properties
quarkus.dynamodb.endpoint-override=http://dynamodb.example:8000
quarkus.dynamodb.aws.credentials.type=static
quarkus.dynamodb.aws.credentials.static-provider.access-key-id=<access-key>
quarkus.dynamodb.aws.credentials.static-provider.secret-access-key=<secret-key>
```

Use the normal AWS credential-provider configuration for deployments. See the [Quarkus Amazon
DynamoDB guide](https://docs.quarkiverse.io/quarkus-amazon-services/dev/amazon-dynamodb.html) for
all client settings. `polaris.persistence.nosql.dynamodb.table-prefix` optionally prefixes the
tables owned by this Polaris deployment.

## Bootstrapping and maintenance

The backend creates its DynamoDB tables during NoSQL bootstrap. Run the [Admin Tool]({{% ref
"../admin-tool" %}}) with the same DynamoDB client configuration as the service. The AWS identity
needs permissions to create, describe, and access the Polaris tables; scope those permissions to
the configured table prefix where possible.

The generated [DynamoDB configuration reference]({{% relref
"../configuration/config-sections/smallrye-polaris_persistence_nosql_dynamodb" %}}) lists the
Polaris-specific settings. Run the regular NoSQL maintenance operations described in the [Admin
Tool]({{% ref "../admin-tool" %}}#nosql-specific-operations) documentation with credentials that
can scan and modify the same DynamoDB tables as Polaris.

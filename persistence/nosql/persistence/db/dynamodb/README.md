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

# DynamoDB persistence backend

This module implements the Polaris NoSQL persistence backend backed by Amazon DynamoDB. For
deployment configuration and bootstrap instructions, see the
[DynamoDB metastore documentation](../../../../../site/content/in-dev/unreleased/metastores/nosql-dynamodb.md).

## Runtime integration

`DynamoDbBackendFactory` supports programmatic use. In a Quarkus runtime,
`DynamoDbBackendBuilder` uses the `DynamoDbClient` supplied by the Quarkus Amazon DynamoDB
extension. Configure the client—region, credentials, endpoint override, and HTTP settings—with
the `quarkus.dynamodb.*` properties. Polaris-specific settings, such as `table-prefix`, use the
`polaris.persistence.nosql.dynamodb.*` prefix.

The programmatic factory resolves credentials with the AWS SDK default credentials provider chain,
independently of whether an endpoint override is configured. When using a local DynamoDB emulator,
provide dummy credentials through a supported source, for example:

```bash
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx
```

Custom AWS endpoints, proxies, and VPC endpoints can therefore use valid credentials from the same
default provider chain. Configuring an endpoint override alone does not select dummy credentials.

The service smoke test starts the Floci AWS emulator and verifies that the Quarkus-managed client
can bootstrap and use the backend. Module integration tests use the same emulator and configure
their own dummy credentials:

```bash
./gradlew :polaris-persistence-nosql-dynamodb:intTest
```

## Bootstrapping

The backend creates and manages its DynamoDB tables during Polaris NoSQL bootstrap. The DynamoDB
identity used by the Admin Tool therefore needs permission to create and manage the prefixed
Polaris tables as well as read and write their contents.

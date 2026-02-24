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
title: NoSQL MongoDB
type: docs
weight: 200
---

{{< alert note >}}
The MongoDB backend is currently in **beta**.
{{< /alert >}}

This implementation uses MongoDB as the persistence backend and leverages the Quarkus MongoDB extension for connection management. Configuration can be done through environment variables or JVM -D flags at startup.

## Basic Configuration

Using environment variables:

```properties
POLARIS_PERSISTENCE_TYPE=nosql
POLARIS_PERSISTENCE_NOSQL_BACKEND=MongoDb
QUARKUS_MONGODB_DATABASE=polaris
QUARKUS_MONGODB_CONNECTION_STRING=mongodb://<username>:<password>@<host>:<port>
```

Using properties file:

```properties
polaris.persistence.type=nosql
polaris.persistence.nosql.backend=MongoDb
quarkus.mongodb.database=polaris
quarkus.mongodb.connection-string=mongodb://<username>:<password>@<host>:<port>
```

## MongoDB Atlas Configuration

For MongoDB Atlas deployments, use the `mongodb+srv://` connection string format:

```properties
polaris.persistence.type=nosql
polaris.persistence.nosql.backend=MongoDb
quarkus.mongodb.database=polaris
quarkus.mongodb.connection-string=mongodb+srv://<username>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority
```

## MongoDB Replica Set Configuration

For replica set deployments:

```properties
polaris.persistence.type=nosql
polaris.persistence.nosql.backend=MongoDb
quarkus.mongodb.database=polaris
quarkus.mongodb.connection-string=mongodb://<username>:<password>@<host1>:<port1>,<host2>:<port2>,<host3>:<port3>/?replicaSet=<rs-name>&authSource=admin
```

## Advanced MongoDB Configuration

Additional MongoDB settings can be configured via Quarkus MongoDB properties:

```properties
quarkus.mongodb.connect-timeout=10S
quarkus.mongodb.read-timeout=30S
quarkus.mongodb.server-selection-timeout=30S
quarkus.mongodb.write-concern.safe=true
quarkus.mongodb.read-preference=primaryPreferred
```

For TLS/SSL connections, add `tls=true` to the connection string:

```properties
quarkus.mongodb.connection-string=mongodb://<username>:<password>@<host>:<port>/?tls=true
```

For more details on available MongoDB configuration options, please refer to the [Quarkus MongoDB configuration reference](https://quarkus.io/guides/mongodb#configuration-reference).

## Bootstrapping Polaris

Before using Polaris with the MongoDB backend, you must bootstrap the metastore to create the necessary collections and initial realm. This is done using the [Admin Tool]({{% ref "../admin-tool" %}}).

Using Docker:

```bash
docker run --rm -it \
  --env="polaris.persistence.type=nosql" \
  --env="polaris.persistence.nosql.backend=MongoDb" \
  --env="quarkus.mongodb.database=polaris" \
  --env="quarkus.mongodb.connection-string=mongodb://<username>:<password>@<host>:<port>" \
  apache/polaris-admin-tool:latest bootstrap -r <realm-name> -c <realm-name>,<client-id>,<client-secret>
```

Using the standalone JAR:

```bash
java \
  -Dpolaris.persistence.type=nosql \
  -Dpolaris.persistence.nosql.backend=MongoDb \
  -Dquarkus.mongodb.database=polaris \
  -Dquarkus.mongodb.connection-string=mongodb://<username>:<password>@<host>:<port> \
  -jar polaris-admin-tool.jar bootstrap -r <realm-name> -c <realm-name>,<client-id>,<client-secret>
```

For more details on the bootstrap command and other administrative operations, see the [Admin Tool]({{% ref "../admin-tool" %}}) documentation.

## Regular maintenance

The Polaris NoSQL backend requires regular maintenance to remove stale data from the backend database.
The frequency of this maintenance depends on the size of the database and the frequency of data updates.
The recommended maintenance frequency is once per day.
It is recommended to schedule maintenance during off-peak hours or to set reasonable scan rates to minimize
the impact on operational availability.
Maintenance can be triggered using the [Admin Tool]({{% ref "../admin-tool" %}}#nosql-specific-operations)

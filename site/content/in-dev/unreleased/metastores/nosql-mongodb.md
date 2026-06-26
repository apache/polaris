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

Polaris supports MongoDB as a persistence backend via the Quarkus MongoDB extension.
Configuration is provided through environment variables or JVM `-D` flags at startup.

For production deployments, MongoDB must be running as a **replica set** (or via MongoDB Atlas).
A single-node standalone MongoDB instance is not supported for production use, as Polaris
requires replica set semantics for its transaction model.

## Prerequisites

- A running MongoDB replica set (minimum 3 nodes recommended) or a MongoDB Atlas cluster
- Network connectivity from the Polaris server to all MongoDB replica set members
- The `apache/polaris-admin-tool` Docker image or the standalone JAR, for bootstrapping

## Production Configuration

### Replica Set

For a self-managed MongoDB replica set, configure Polaris with all replica set member
hostnames in the connection string:

```properties
polaris.persistence.type=nosql
polaris.persistence.nosql.backend=MongoDb
quarkus.mongodb.database=<database-name>
quarkus.mongodb.connection-string=mongodb://<username>:<password>@<host1>:<port1>,<host2>:<port2>,<host3>:<port3>/?replicaSet=<rs-name>&authSource=admin
```

You must also tell Polaris which realms to serve at startup. The realm name must exactly
match the name used during bootstrapping (case-sensitive):

```properties
polaris.realm-context.realms=<realm-name>
```

{{< alert warning >}}
The realm name is case-sensitive. If you bootstrap a realm as `polaris`, you must configure
`polaris.realm-context.realms=polaris` (lowercase) when starting the server. A mismatch will
cause `Unknown realm` errors at runtime.
{{< /alert >}}

### MongoDB Atlas

For MongoDB Atlas deployments, use the `mongodb+srv://` connection string format:

```properties
polaris.persistence.type=nosql
polaris.persistence.nosql.backend=MongoDb
quarkus.mongodb.database=<database-name>
quarkus.mongodb.connection-string=mongodb+srv://<username>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority
polaris.realm-context.realms=<realm-name>
```

### TLS/SSL

For encrypted connections, add `tls=true` to the connection string:

```properties
quarkus.mongodb.connection-string=mongodb://<username>:<password>@<host1>:<port1>,<host2>:<port2>,<host3>:<port3>/?replicaSet=<rs-name>&tls=true
```

### Advanced Tuning

Additional MongoDB settings can be configured via Quarkus MongoDB properties:

```properties
quarkus.mongodb.connect-timeout=10S
quarkus.mongodb.read-timeout=30S
quarkus.mongodb.server-selection-timeout=30S
quarkus.mongodb.write-concern.safe=true
quarkus.mongodb.read-preference=primaryPreferred
```

For the full list of available options, refer to the
[Quarkus MongoDB configuration reference](https://quarkus.io/guides/mongodb#configuration-reference).

## Bootstrapping Polaris

Before starting Polaris for the first time, you must bootstrap the metastore. This creates
the necessary MongoDB collections (`objs` and `refs`) and an initial realm with root
credentials. Bootstrapping is a one-time operation performed using the
[Admin Tool]({{% ref "../admin-tool" %}}).

{{< alert note >}}
The bootstrap command connects directly to MongoDB and does not require the Polaris server
to be running.
{{< /alert >}}

Using Docker:

```bash
docker run --rm -it \
  --network=host \
  --env="polaris.persistence.type=nosql" \
  --env="polaris.persistence.nosql.backend=MongoDb" \
  --env="quarkus.mongodb.database=<database-name>" \
  --env="quarkus.mongodb.connection-string=mongodb://<host1>:<port1>,<host2>:<port2>,<host3>:<port3>/?replicaSet=<rs-name>" \
  apache/polaris-admin-tool:latest bootstrap \
  -r <realm-name> \
  -c <realm-name>,<client-id>,<client-secret>
```

Using the standalone JAR:

```bash
java \
  -Dpolaris.persistence.type=nosql \
  -Dpolaris.persistence.nosql.backend=MongoDb \
  -Dquarkus.mongodb.database=<database-name> \
  -Dquarkus.mongodb.connection-string=mongodb://<host1>:<port1>,<host2>:<port2>,<host3>:<port3>/?replicaSet=<rs-name> \
  -jar polaris-admin-tool.jar bootstrap \
  -r <realm-name> \
  -c <realm-name>,<client-id>,<client-secret>
```

On success, you will see:

```
Realm '<realm-name>' successfully bootstrapped.
Bootstrap completed successfully.
```

You can verify the collections were created by connecting to MongoDB directly:

```bash
mongosh --eval "db.getSiblingDB('<database-name>').getCollectionNames()"
# Expected output: [ 'objs', 'refs' ]
```

For more details on the bootstrap command and other administrative operations, see the
[Admin Tool]({{% ref "../admin-tool" %}}) documentation.


## Starting Polaris

After bootstrapping, start the Polaris server with the same MongoDB connection settings.
The realm name must match exactly what was used during bootstrapping:

Using Docker:

```bash
docker run -d \
  --name polaris \
  --network=host \
  --env="polaris.persistence.type=nosql" \
  --env="polaris.persistence.nosql.backend=MongoDb" \
  --env="quarkus.mongodb.database=<database-name>" \
  --env="quarkus.mongodb.connection-string=mongodb://<host1>:<port1>,<host2>:<port2>,<host3>:<port3>/?replicaSet=<rs-name>" \
  --env="polaris.realm-context.realms=<realm-name>" \
  apache/polaris:latest
```

Polaris starts on port `8181` (API) and `8182` (management/health).

## Verifying the Setup

After bootstrapping and starting Polaris, verify the MongoDB connection via the health
endpoint (available on the management port, default `8182`):

```bash
curl http://localhost:8182/q/health
```

A healthy response looks like:

```json
{
  "status": "UP",
  "checks": [
    {
      "name": "MongoDB connection health check",
      "status": "UP",
      "data": {
        "<default>": "OK",
        "<default-reactive>": "OK"
      }
    }
  ]
}
```

To verify end-to-end, obtain an access token using the credentials from bootstrapping:

```bash
curl -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -H "Polaris-Realm: <realm-name>" \
  -d "grant_type=client_credentials&client_id=<client-id>&client_secret=<client-secret>&scope=PRINCIPAL_ROLE:ALL"
```

A successful response returns a JWT `access_token`.

## Regular Maintenance

The Polaris NoSQL backend requires regular maintenance to remove stale data from MongoDB.
Maintenance is performed using the `nosql maintenance-run` subcommand of the
[Admin Tool]({{% ref "../admin-tool" %}}#nosql-specific-operations).

The recommended frequency is once per day, scheduled during off-peak hours to minimize
impact on operational availability.

Using Docker:

```bash
docker run --rm -it \
  --network=host \
  --env="polaris.persistence.type=nosql" \
  --env="polaris.persistence.nosql.backend=MongoDb" \
  --env="quarkus.mongodb.database=<database-name>" \
  --env="quarkus.mongodb.connection-string=mongodb://<host1>:<port1>,<host2>:<port2>,<host3>:<port3>/?replicaSet=<rs-name>" \
  apache/polaris-admin-tool:latest nosql maintenance-run
```

The output shows a summary of scanned, retained, and purged objects per realm. A typical
healthy run on a fresh instance will show all objects as `too new` to purge, which is
expected.

To inspect past maintenance runs or current maintenance configuration, use the related
subcommands:

```bash
# Show maintenance configuration and statistics
docker run --rm -it \
  --network=host \
  --env="polaris.persistence.type=nosql" \
  --env="polaris.persistence.nosql.backend=MongoDb" \
  --env="quarkus.mongodb.database=<database-name>" \
  --env="quarkus.mongodb.connection-string=mongodb://<host1>:<port1>,<host2>:<port2>,<host3>:<port3>/?replicaSet=<rs-name>" \
  apache/polaris-admin-tool:latest nosql maintenance-info

# Show log of past maintenance runs
docker run --rm -it \
  --network=host \
  --env="polaris.persistence.type=nosql" \
  --env="polaris.persistence.nosql.backend=MongoDb" \
  --env="quarkus.mongodb.database=<database-name>" \
  --env="quarkus.mongodb.connection-string=mongodb://<host1>:<port1>,<host2>:<port2>,<host3>:<port3>/?replicaSet=<rs-name>" \
  apache/polaris-admin-tool:latest nosql maintenance-log
```

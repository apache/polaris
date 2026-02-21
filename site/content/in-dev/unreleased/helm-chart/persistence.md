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

title: Persistence
linkTitle: Persistence
weight: 400
---

This page describes how to configure persistence for Polaris when deploying with the Helm chart.
For general information on persistence in Polaris, see the [Metastores]({{% ref "../metastores" %}}) page.

Polaris supports three metastore implementations:

- [In-Memory](#in-memory-metastore) - for testing only
- [PostgreSQL (JDBC)](#postgresql-jdbc-metastore) - recommended for production
- [MongoDB (NoSQL)](#mongodb-nosql-metastore) - experimental (beta)

{{< alert warning >}}
The default `in-memory` metastore is **not suitable for production**. Data will be lost when pods restart, and you cannot run multiple replicas.
{{< /alert >}}

## In-Memory Metastore

The in-memory metastore is the default and requires no configuration. It is useful for quick testing and development:

```yaml
persistence:
  type: in-memory
```

{{< alert note >}}
When using the in-memory metastore, you must set `replicaCount: 1` as data is not shared between pods. Also, you cannot enable horizontal autoscaling.
{{< /alert >}}

## PostgreSQL (JDBC) Metastore

The PostgreSQL metastore stores all Polaris metadata in a PostgreSQL database. This is the recommended metastore for production deployments.

### Prerequisites

- A PostgreSQL database
- Network connectivity from the Kubernetes cluster to the database
- Database credentials with appropriate permissions

### Creating the Secret

Create a Kubernetes secret containing the database connection details:

```bash
kubectl create secret generic polaris-persistence \
  --namespace polaris \
  --from-literal=username=<db-username> \
  --from-literal=password=<db-password> \
  --from-literal=jdbcUrl=jdbc:postgresql://<db-host>:5432/<db-name>
```

The secret must contain three keys:
- `username`: The database username
- `password`: The database password
- `jdbcUrl`: The JDBC connection URL

### Configuring the Chart

Configure the chart to use the PostgreSQL metastore:

```yaml
persistence:
  type: relational-jdbc
  relationalJdbc:
    secret:
      name: "polaris-persistence"
      username: "username"      # Key in secret containing the username
      password: "password"      # Key in secret containing the password
      jdbcUrl: "jdbcUrl"        # Key in secret containing the JDBC URL
```

### Connection Pool Settings

For production deployments, you may want to tune the connection pool settings using the `advancedConfig` section:

```yaml
advancedConfig:
  quarkus.datasource.jdbc.min-size: "5"
  quarkus.datasource.jdbc.max-size: "20"
  quarkus.datasource.jdbc.acquisition-timeout: "30S"
  quarkus.datasource.jdbc.idle-removal-interval: "5M"
```

See the [Quarkus Datasource configuration reference](https://quarkus.io/guides/datasource#configuration-reference) for all available options.

## MongoDB (NoSQL) Metastore

{{< alert note >}}
The MongoDB NoSQL metastore is currently in **beta**.
{{< /alert >}}

The MongoDB metastore stores all Polaris metadata in a MongoDB database.

### Prerequisites

- A MongoDB database
- Network connectivity from the Kubernetes cluster to the database
- A MongoDB connection string with appropriate credentials

### Creating the Secret

Create a Kubernetes secret containing the MongoDB connection string, for example:

```bash
kubectl create secret generic polaris-mongodb \
  --namespace polaris \
  --from-literal=connectionString='mongodb://<username>:<password>@<host>:<port>'
```

For MongoDB Atlas or replica sets, use the appropriate connection string format:

```bash
# MongoDB Atlas
kubectl create secret generic polaris-mongodb \
  --namespace polaris \
  --from-literal=connectionString='mongodb+srv://<username>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority'

# Replica Set
kubectl create secret generic polaris-mongodb \
  --namespace polaris \
  --from-literal=connectionString='mongodb://<username>:<password>@<host1>:<port1>,<host2>:<port2>,<host3>:<port3>/?replicaSet=<rs-name>&authSource=admin'
```

### Configuring the Chart

Configure the chart to use the NoSQL metastore with `MongoDb` backend:

```yaml
persistence:
  type: nosql
  nosql:
    backend: MongoDb
    database: polaris
    secret:
      name: "polaris-mongodb"
      connectionString: "connectionString"  # Key in secret containing the connection string
```

### Configuration Options

| Setting                                     | Default            | Description                                                                     |
|---------------------------------------------|--------------------|---------------------------------------------------------------------------------|
| `persistence.nosql.backend`                 | `MongoDb`          | The NoSQL backend type. Use `MongoDb` for production or `InMemory` for testing. |
| `persistence.nosql.database`                | `polaris`          | The MongoDB database name to use.                                               |
| `persistence.nosql.secret.name`             | `""`               | The name of the secret containing the connection string.                        |
| `persistence.nosql.secret.connectionString` | `connectionString` | The key in the secret holding the MongoDB connection string.                    |

### Advanced MongoDB Configuration

For advanced MongoDB settings, use the `advancedConfig` section to pass Quarkus MongoDB configuration properties:

```yaml
advancedConfig:
  quarkus.mongodb.connect-timeout: "10S"
  quarkus.mongodb.read-timeout: "30S"
  quarkus.mongodb.server-selection-timeout: "30S"
  quarkus.mongodb.write-concern.safe: "true"
  quarkus.mongodb.read-preference: "primaryPreferred"
```

See the [Quarkus MongoDB configuration reference](https://quarkus.io/guides/mongodb#configuration-reference) for all available options.

## Production Recommendations

When deploying Polaris in production, consider the following recommendations:

1. **Enable TLS/SSL**: Ensure database connections are encrypted. For PostgreSQL, add `?sslmode=require` to the JDBC URL. For MongoDB, use `tls=true` in the connection string.

2. **Use strong credentials**: Generate strong, unique passwords for database access and rotate them regularly.

3. **Configure connection pooling**: Tune connection pool settings based on your expected load and database capacity.

4. **Set up monitoring**: Monitor database performance and connection metrics to identify issues early.

5. **Plan for backups**: Implement a backup strategy appropriate for your data retention requirements.

6. **Test failover**: If using a high-availability database setup, test failover scenarios to ensure your application handles them gracefully.

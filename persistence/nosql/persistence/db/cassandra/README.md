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

# Cassandra persistence backend

This module implements the Polaris NoSQL persistence backend backed by Apache Cassandra. It
uses the Apache Cassandra Java driver directly: Polaris creates and configures a `CqlSession`;
it does not use a Quarkus Cassandra extension or the driver's Typesafe Config integration.

For deployment configuration and bootstrapping instructions, see the
Cassandra metastore documentation.

## Runtime integration

`CassandraBackendFactory` supports programmatic, non-Quarkus use. In a Quarkus runtime,
`CassandraClientProducer` supplies the application-scoped `CqlSession`; it also integrates:

- the Quarkus TLS Registry;
- Micrometer driver metrics, when a `MeterRegistry` is available; and
- the Cassandra readiness health check when Cassandra is the configured NoSQL backend.

The driver is configured from `CassandraConfiguration` and its `driver` subsection. Polaris
builds an in-memory driver configuration, so it neither reads nor requires a driver
`application.conf` resource. The generic `driver` subsection maps exact Java-driver option paths
to their typed driver values. Polaris owns the request timeout, consistency levels, metrics,
and—when selected—TLS.
Refer to the
[Apache Cassandra Java Driver configuration manual](https://github.com/apache/cassandra-java-driver/blob/4.x/manual/core/configuration/README.md)
for the driver option names and semantics.

## Consistency

`polaris.persistence.nosql.cassandra.consistency-level` defaults to `LOCAL_QUORUM` and also
supports `QUORUM` and `ALL`. It controls object reads, writes, backend scans, and the commit phase
of conditional mutations.

`polaris.persistence.nosql.cassandra.serial-consistency-level` defaults to `LOCAL_SERIAL` and
also supports `SERIAL`. It controls reference reads and the Paxos phase of conditional mutations.
Reference reads resolve unfinished Paxos proposals before returning and are idempotent, allowing
the driver to retry them. Object reads retain the regular consistency level.

All Polaris instances accessing a realm must use compatible consistency settings. The default
serial consistency confines Paxos to the local data center; selecting `SERIAL` alone does not
establish support for writers in multiple data centers.

Reference creation, updates, and deletion all use lightweight transactions. Ordinary object
writes and deletes are intended for immutable objects whose IDs are not reused. Mutable,
versioned objects must use the conditional insert, update, and delete operations throughout
their lifecycle; mixing ordinary deletes with those operations can produce conflicting
timestamps.

## Authentication

`auth.provider-name` defaults to `default`. When both `auth.username` and `auth.password` are
configured, Polaris creates the default plaintext driver authentication provider. An optional
`auth.authorization-id` is passed to that provider for servers that support proxy authentication.

For custom authentication, an integrator supplies a CDI `AuthProvider` bean qualified with
`@Identifier("name")` from `io.smallrye.common.annotation` and configures
`auth.provider-name=name`. Native Java-driver
authentication options under `driver` cannot be combined with Polaris authentication settings.

## TLS and mTLS

For a Quarkus deployment, configure a named TLS Registry bucket and select it with
`polaris.persistence.nosql.cassandra.tls-configuration-name`. For mTLS with PEM files mounted
from a secret, the bucket needs the client certificate, private key, and the CA certificate(s)
trusted for the Cassandra server:

```properties
polaris.persistence.nosql.cassandra.tls-configuration-name=cassandra
quarkus.tls.cassandra.key-store.pem.client.cert=/etc/cassandra-tls/client.crt
quarkus.tls.cassandra.key-store.pem.client.key=/etc/cassandra-tls/client.key
quarkus.tls.cassandra.trust-store.pem.certs=/etc/cassandra-tls/ca.crt
```

Keep hostname validation enabled (the default) and configure the trust store with the intended
CA certificates. Do not use `quarkus.tls.<name>.trust-all` outside a controlled test environment.
The selected TLS Registry configuration is the only TLS configuration used by this backend; do
not set Java-driver TLS options under `driver` at the same time.

## Bootstrapping

Have a DBA create the configured Cassandra keyspace with the deployment-appropriate replication
strategy before using the Admin Tool to bootstrap the metastore. The backend creates its own
tables but deliberately does not create a keyspace, because replication strategy and topology are
deployment decisions. The managed schema requires Cassandra 5.0 or later because it uses UCS;
the public metastore documentation describes the Cassandra 4.x compatibility path.

The server also calls schema setup during startup and creates missing tables. Run the Admin Tool
from one administrative process and wait for schema initialization to finish before starting
server replicas. `CREATE TABLE IF NOT EXISTS` does not make concurrent schema initialization safe
on Cassandra versions before 6.0. With pre-created tables, server startup only validates the schema
and does not require table-creation permissions. Realm deletion is not supported by this backend.

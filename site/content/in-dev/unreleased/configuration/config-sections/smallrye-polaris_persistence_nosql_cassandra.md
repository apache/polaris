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
title: smallrye-polaris_persistence_nosql_cassandra
build:
  list: never
  render: never
---

Polaris persistence, Cassandra backend-specific configuration. 

Configure Java-driver settings under `polaris.persistence.nosql.cassandra.driver`, using  exact driver option paths. For example, `driver.basic.contact-points=cassandra.example:9042` configures the driver option `basic.contact-points` .   

For Quarkus TLS or mTLS, set `tls-configuration-name` and configure the selected `quarkus.tls.<name>.*` TLS Registry configuration. This mapping does not contain certificate,  private-key, trust CA, protocol, or cipher-suite settings. A driver `application.conf` resource is neither needed nor consulted.   

Standalone users can configure the connection settings in this mapping, but cannot use `tls-configuration-name` , which requires Quarkus's TLS Registry.   

Additional Cassandra Java driver options can be configured under `driver`, using the  exact driver option path. For example, `driver.basic.request.page-size=500` configures the  driver option `basic.request.page-size`. This is an in-memory driver configuration; a  driver `application.conf` is not used. The Polaris-owned request timeout and consistency  settings cannot be configured through this subsection. In Quarkus, metrics default to session  `connected-nodes,cql-requests` and node `pool.open-connections` plus selected request  and connection error counters. Override `driver.advanced.metrics.session.enabled` and  `driver.advanced.metrics.node.enabled` with explicit metric-name lists; an empty list  disables that category. The metrics factory and naming settings are managed by Polaris. When  `tls-configuration-name` is set, configure TLS settings exclusively through the Quarkus TLS  Registry rather than this subsection. Native driver authentication options can be used only when  the default `auth.provider-name` is selected and `auth.username`, `auth.password` , and `auth.authorization-id` are absent.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.persistence.nosql.cassandra.keyspace` | `polaris` | `string` | The Cassandra keyspace containing the Polaris persistence tables.  |
| `polaris.persistence.nosql.cassandra.tls-hostname-validation` | `true` | `boolean` | Whether to verify that the Cassandra server certificate identifies the contacted host. <br><br>Only applies when `tls-configuration-name` selects a Quarkus TLS configuration.  Otherwise, configure hostname validation through the native driver TLS options.   <br><br>Disabling this option is unsafe and should not be used in production. |
| `polaris.persistence.nosql.cassandra.request-timeout` |  | `duration` | Session-wide Cassandra request timeout. Defaults to the larger of `ddl-timeout` and  `dml-timeout` when omitted.  |
| `polaris.persistence.nosql.cassandra.auth.provider-name` | `default` | `string` | The named CDI authentication provider to use in Quarkus deployments. <br><br>Defaults to `default`. With the default provider, Polaris uses username and password  when both are configured, or leaves authentication to the native driver when they are absent.  |
| `polaris.persistence.nosql.cassandra.auth.username` |  | `string` | Plaintext authentication username, used only with the `default` provider.   |
| `polaris.persistence.nosql.cassandra.auth.password` |  | `string` | Plaintext authentication password, used only with the `default` provider.   |
| `polaris.persistence.nosql.cassandra.auth.authorization-id` |  | `string` | Optional plaintext authentication authorization ID, used only with the `default` provider.  <br><br>This option is effective only with servers that support proxy authentication. Apache  Cassandra ignores it.  |
| `polaris.persistence.nosql.cassandra.driver.`_`<name>`_ |  | `string` | Additional Cassandra Java driver options, keyed by their exact driver option paths.  |
| `polaris.persistence.nosql.cassandra.tls-configuration-name` |  | `string` | The named Quarkus TLS Registry configuration to use for TLS or mTLS.  |
| `polaris.persistence.nosql.cassandra.consistency-level` | `LOCAL_QUORUM` | <span title="ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_ONE, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL"><code>enum (ANY, ONE, TWO, ...)</code></span> | Consistency for metadata reads and writes. Supported values are `LOCAL_QUORUM`, `QUORUM` , and `ALL`; weaker levels do not ensure quorum overlap. All Polaris instances  accessing a realm must use compatible consistency settings.  |
| `polaris.persistence.nosql.cassandra.serial-consistency-level` | `LOCAL_SERIAL` | <span title="ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_ONE, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL"><code>enum (ANY, ONE, TWO, ...)</code></span> | Serial consistency for conditional mutations: `LOCAL_SERIAL` or `SERIAL`. The  default confines Paxos to the local data center; selecting `SERIAL` alone does not  establish support for a deployment with writers in multiple data centers.  |
| `polaris.persistence.nosql.cassandra.ddl-timeout` | `PT5S` | `duration` | Timeout used when creating tables.  |
| `polaris.persistence.nosql.cassandra.dml-timeout` | `PT3S` | `duration` | Timeout used for queries and updates.  |

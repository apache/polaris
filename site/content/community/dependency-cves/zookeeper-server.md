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
title: "Apache ZooKeeper: vulnerabilities in the ZooKeeper server"
linkTitle: ZooKeeper server CVEs
type: docs
date: 2026-11-06
cve:
  - CVE-2026-115448
  - CVE-2026-100302
  - CVE-2026-100300
  - CVE-2026-100252
  - CVE-2026-100235
  - CVE-2026-84501
  - CVE-2026-84439
  - CVE-2026-79993
  - CVE-2026-59969
  - CVE-2026-59739
versions: "1.8.0"
components:
  - pkg:maven/org.apache.zookeeper/zookeeper@3.8.6
  - pkg:maven/org.apache.zookeeper/zookeeper-jute@3.8.6
analysis:
  state: not_affected
  justification: code_not_reachable
---
These CVEs affect Apache ZooKeeper 3.8.6 (see the
[ZooKeeper security page](https://zookeeper.apache.org/security.html)). All of them are in the
ZooKeeper **server**: quorum TLS hostname verification, ACL and SASL authorization checks, the
operational and audit logs, the handling of `multiRead`, quota and `SetWatches` requests, and
snapshot serialization.

Polaris is **not affected**. The `zookeeper` JAR, which contains both the ZooKeeper client and
server, reaches the Polaris server through `hadoop-auth`, a dependency of the Apache Ranger
authorizer and of the Hadoop catalog federation. Polaris does not run a ZooKeeper server: the
libraries shipped with Polaris (Ranger, SolrJ, Curator, Hadoop) only use the ZooKeeper client, and
no code shipped with Polaris starts a ZooKeeper server.

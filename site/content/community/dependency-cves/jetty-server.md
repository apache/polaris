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
title: "Eclipse Jetty: vulnerabilities in the Jetty HTTP server"
linkTitle: Jetty server CVEs
type: docs
date: 2026-11-06
cve:
  - CVE-2024-13009
  - CVE-2026-6790
versions: "1.8.0"
components:
  - pkg:maven/org.eclipse.jetty/jetty-client@9.4.56.v20240826
  - pkg:maven/org.eclipse.jetty/jetty-http@9.4.56.v20240826
  - pkg:maven/org.eclipse.jetty/jetty-io@9.4.56.v20240826
  - pkg:maven/org.eclipse.jetty/jetty-util@9.4.56.v20240826
  - pkg:maven/org.eclipse.jetty/jetty-client@9.4.58.v20250814
  - pkg:maven/org.eclipse.jetty/jetty-http@9.4.58.v20250814
  - pkg:maven/org.eclipse.jetty/jetty-io@9.4.58.v20250814
  - pkg:maven/org.eclipse.jetty/jetty-util-ajax@9.4.58.v20250814
  - pkg:maven/org.eclipse.jetty/jetty-webapp@9.4.58.v20250814
  - pkg:maven/org.eclipse.jetty/jetty-xml@9.4.58.v20250814
  - pkg:maven/org.eclipse.jetty.websocket/websocket-api@9.4.58.v20250814
  - pkg:maven/org.eclipse.jetty.websocket/websocket-client@9.4.58.v20250814
  - pkg:maven/org.eclipse.jetty.websocket/websocket-common@9.4.58.v20250814
analysis:
  state: not_affected
  justification: code_not_present
---
CVE-2024-13009 (`GzipHandler` exposing part of a request body to a separate request) and
CVE-2026-6790 (HTTP authority and `Host` header mismatch) are vulnerabilities in the Eclipse Jetty
HTTP server, in the `jetty-server` module.

Polaris is **not affected**. Polaris uses Quarkus and Vert.x as its HTTP server, not Jetty, and does
not ship `jetty-server`. The Polaris server ships `jetty-client`, `jetty-http`, `jetty-io` and
`jetty-util` 9.4.56, through the Solr audit destination of the Apache Ranger authorizer, and the
`hadoop-client-runtime` JAR used by the Hadoop catalog federation bundles relocated copies of other
Jetty 9.4.58 modules. None of these JARs contains the `jetty-server` classes. Scanners that match
Jetty CVEs on the product name rather than on the module flag these JARs anyway.

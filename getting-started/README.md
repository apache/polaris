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

# Getting Started with Apache Polaris

You can quickly get started with Polaris by playing with the docker-compose examples provided in
this directory. Each example has detailed instructions.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/) 
- [jq](https://stedolan.github.io/jq/download/) (for some examples)

## Getting Started Examples

- [Spark](spark): An example that uses an in-memory metastore, automatically bootstrapped, with
  Apache Spark and a Jupyter notebook.

- [Telemetry](telemetry): An example that includes Prometheus and Jaeger to collect metrics and
  traces from Apache Polaris. This example automatically creates a `polaris_demo` catalog.

- [Eclipselink](eclipselink): An example that uses an Eclipselink metastore and a Postgres
  database. The realm is bootstrapped with the Polaris Admin tool. This example also creates a
  `polaris_quickstart` catalog, and offers the ability to run Spark SQL and Trino queries. Finally, it shows how to
  attach a debugger to the Polaris server.

- [Keycloak](keycloak): An example that uses Keycloak as an external identity provider (IDP) for
  authentication.
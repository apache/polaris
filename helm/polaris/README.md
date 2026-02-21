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

# Helm Chart for Apache Polaris

[![Artifact HUB](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/apache-polaris)](https://artifacthub.io/packages/search?repo=apache-polaris)

[Apache Polaris](https://polaris.apache.org/) is an open-source, fully-featured catalog for Apache Iceberg™. It 
implements Iceberg's REST API, enabling seamless multi-engine interoperability across a wide range of platforms, 
including Apache Doris™, Apache Flink®, Apache Spark™, Dremio® OSS, StarRocks, and Trino.

## Requirements

- Kubernetes 1.33+ cluster
- Helm 3.x or 4.x

## Features

* Apache Iceberg REST catalog implementing the Iceberg REST specification
* Support for multiple table formats, including Iceberg, Delta Lake, Hudi, and Lance
* Multi-engine interoperability for Spark, Trino, Flink, and other Iceberg-compatible engines
* Centralized security and governance with principals, roles, and fine-grained privileges
* Centralized catalog, namespace, and table management
* Kubernetes-native deployment with support for horizontal scaling, Ingress, and Gateway API
* Production-ready observability with health checks and metrics
* Open source and vendor neutral, governed by the Apache Polaris PMC under the Apache Software Foundation

## Documentation

Full documentation for Helm Chart lives [on the website](https://polaris.apache.org/in-dev/unreleased/helm/).

## Contributing

Want to help build Apache Polaris? Check out our [contributing documentation](https://polaris.apache.org/community/contributing-guidelines/).

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
title: Building a Custom Server Based on Polaris
type: docs
weight: 1005
---

This page proposes a method for building custom servers based on Apache Polaris.

Polaris produces several jars. These jars may be used to build custom catalog server 
according to the terms of the license included in Polaris distributions.

## General Principles

* Create a module in your project (Maven, Gradle or any other build tool) for a
[Quarkus Application](https://code.quarkus.io/).
* Include `polaris-runtime-service` as a runtime or compile-time dependency.
* Include / exclude other dependencies according to the project's needs (e.g., an alternative JDBC driver).
* Do not include `polaris-runtime-defaults` or `polaris-server` as dependencies.
* Provide your own `application.properties` file for your Quarkus build.
* Define your own Quarkus Application Name and Version (do not use "Apache Polaris" as the application name).

## Background

The `polaris-server` module is intended to be a particular way to build a Polaris server. It includes
dependencies that are considered essential for the Apache Polaris distribution. This is not necessarily
the case for downstream builds, which may want to exclude some features and include others (perhaps
custom implementations of some Polaris SPI interfaces).

The `polaris-runtime-defaults` module is intended to hold `application.properties` configured for the 
default Apache Polaris distribution. While it may be tempting to reuse it in a downstream build and
add overrides for some properties, practice shows that in this case it becomes very hard to predict
the effects of property customizations in the downstream build. While it may be relevant to Gradle builds
only, it is much more clear to have the full set of properties in a custom `application.properties` file
without any other `application.properties` files in transitive dependencies.

The `polaris-runtime-service` module contains standard service endpoint implementations and is intended
to be reused. Custom [CDI](https://quarkus.io/guides/cdi-reference) beans may be used in downstream
projects to override / enhance beans provided by `polaris-runtime-service`.

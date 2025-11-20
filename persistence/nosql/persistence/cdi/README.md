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

# CDI functionality for Polaris NoSQL persistence

NoSQL persistence provides three modules for CDI:
* A module for Quarkus, which Polaris used for production deployments.
* A module for Weld, which is used for testing purposes.
* A module with shared CDI functionality for both Quarkus and Weld.

Polaris runs on top of the Quarkus framework, leveraging CDI.

To build and run tests in a more performant way, many test classes in Polaris NoSQL persistence
uses the CDI reference implementation Weld instead of Quarkus, as it requires no intermediate
augmentation (think: Quarkus build).

The biggest difference between the Quarkus and Weld variants is the way how database specific
`Backend` instances are produced, because the Weld variant targets testing purposes.
* Weld locates the `Backend` instances using Java's service loader mechanism via 
  `org.apache.polaris.persistence.nosql.api.backend.BackendLoader.findFactoryByName()`, which is
  also what the the NoSQL persistence JUnit test extension uses.
* In Quarkus, the `Backend` instances are located using a CDI identifier-based mechanism.
  There are also backend specific builders that leverage Quarkus extensions for the respective
  database backends.
  The Quarkus variant also adds OpenTelemetry instrumentation to the `Backend` instances.

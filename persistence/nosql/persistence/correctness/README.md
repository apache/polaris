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

# Polaris NoSQL Persistence Correctness Tests

Collection of tests that use JUnit to verify the correctness of the persistence implementation.

`:polaris-persistence-nosql-correctness` contains a couple of test suites with test tasks named `correctnessTest-XYZ`,
where `XYZ` determines the database kind. Those tests use backends on the Java heap or using testcontainers and
are dependents of Gradle's `check` task.

The `correctnessManualTest` task however is meant to be run _manually_ against a separate database/cluster setup,
providing the necessary backend configuration via system properties. For example:
```bash
./gradlew :polaris-persistence-nosql-correctness:correctnessManualTest \
  -Dpolaris.persistence.backend=MongoDb \
  -Dpolaris.persistence.backend.mongodb.uri=mongodb://localhost:27017/test
  -Dpolaris.persistence.backend.mongodb.databaseName=polaris_mongo_test
```
See also the Docker Compose example in the [`docker`](../docker) directory.

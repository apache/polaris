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

Some container run commands...

```bash
podman run --rm -ti \
  --name demo_mongo \
  -p 27017:27017 \
  docker.io/library/mongo:8.0.5
```

```bash
./gradlew :polaris-persistence-nosql-benchmark:jmhJar && java \
  -Dpolaris.persistence.backend.type=InMemory \
  -jar persistence/benchmark/build/libs/polaris-persistence-nosql-benchmark-1.0.0-incubating-SNAPSHOT-jmh.jar
```

```bash
./gradlew :polaris-persistence-nosql-benchmark:jmhJar && java \
  -Dpolaris.persistence.backend.type=MongoDb \
  -Dpolaris.persistence.backend.mongodb.connection-string=mongodb://localhost:27017/ \
  -Dpolaris.persistence.backend.mongodb.database-name=test \
  -jar persistence/benchmark/build/libs/polaris-persistence-nosql-benchmark-1.0.0-incubating-SNAPSHOT-jmh.jar
```

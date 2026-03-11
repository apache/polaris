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
---

# Guides CI setup, this is NOT a guide

**This page is not rendered on the website.**

The directory name starting with a `.` ensures that this page is executed by the guides integration tests
before any other guide's code blocks.

## Eager code block execution

This code block is eagerly executed here to further skip the repeated and expensive Polaris image builds
present in the guides' code blocks.

```shell
./gradlew \
   :polaris-server:assemble \
   :polaris-server:quarkusAppPartsBuild --rerun \
   :polaris-admin:assemble \
   :polaris-admin:quarkusAppPartsBuild --rerun \
   -Dquarkus.container-image.build=true
```

## Ignored code blocks

These code blocks are not executed in guide tests because the Docker images have already been generated.

The `skip_all` directive is used to skip this and all following executions of the same code block,
irrespective whether `skip_all` is present or not.

> **Note:** The `skip_all` directive is only supported by the `shell` code block.

> **Note:** The code block must be _exactly_ the same, including all whitespaces.

```shell skip_all
./gradlew \
   :polaris-server:assemble \
   :polaris-server:quarkusAppPartsBuild --rerun \
   -Dquarkus.container-image.build=true
```

```shell skip_all
./gradlew \
   :polaris-server:assemble \
   :polaris-server:quarkusAppPartsBuild --rerun \
   :polaris-admin:assemble \
   :polaris-admin:quarkusAppPartsBuild --rerun \
   -Dquarkus.container-image.build=true
```

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

# Polaris Quarkus Server

This module contains the Quarkus-based Polaris server main artifact.

## Archive distribution

Building this module will create a zip/tar distribution with the Polaris server.

To build the distribution, you can use the following command:

```shell
./gradlew :polaris-server:build
```

You can manually unpack and run the distribution archives:

```shell
cd runtime/server/build/distributions
unzip polaris-server-<version>.zip
cd polaris-server-<version>
java -jar quarkus-run.jar
```

## Docker image

To also build the Docker image, you can use the following command (a running Docker daemon is
required):

```shell
./gradlew \
  :polaris-server:assemble \
  :polaris-server:quarkusAppPartsBuild --rerun \
  -Dquarkus.container-image.build=true
```

If you need to customize the Docker image, for example to push to a local registry, you can use the
following command:

```shell
./gradlew \
  :polaris-server:assemble \
  :polaris-server:quarkusAppPartsBuild --rerun \
  -Dquarkus.container-image.build=true \
  -Dquarkus.container-image.registry=localhost:5001 \
  -Dquarkus.container-image.group=apache \
  -Dquarkus.container-image.name=polaris-local
```

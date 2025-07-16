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

# Polaris Admin Tool

This module contains a maintenance tool for performing administrative tasks on the Polaris database.
It is a Quarkus application that can be used to perform various maintenance tasks targeting the
Polaris database directly.

## Archive distribution

Building this module will create a zip/tar distribution with the Polaris server.

To build the distribution, you can use the following command:

```shell
./gradlew :polaris-admin:build
```

You can manually unpack and run the distribution archives:

```shell
cd runtime/admin/build/distributions
unzip polaris-admin-<version>.zip
cd polaris-admin-<version>
java -jar polaris-admin-<version>-runner.jar
```

## Docker image

To also build the Docker image, you can use the following command:

```shell
./gradlew \
  :polaris-admin:assemble \
  :polaris-admin:quarkusAppPartsBuild --rerun \
  -Dquarkus.container-image.build=true
```

## Running the Admin Tool

The admin tool can be run from the command line using the following command:

```shell
java -jar polaris-admin-<version>-runner.jar --help
```

Using the Docker image, you can run the admin tool with the following command:

```shell
docker run --rm -it apache/polaris-admin-tool:<version> --help
```

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
title: Getting Started with Apache Polaris
linkTitle: Getting Started
type: docs
weight: 101
---

This guide will help you get started with Apache Polaris using the binary distribution, which is the fastest way to set up a Polaris server.

## Prerequisites

Before you begin, ensure you have the following:

- Java SE 21 or higher installed
- Verify Java installation:

```bash
java -version
```

## Step 1: Download and Extract the Binary Distribution

1. Visit the [Apache Polaris downloads page](https://polaris.apache.org/downloads/).

2. Download the latest binary distribution (tar.gz file) and extract it to your desired directory:

```bash
tar -xzf apache-polaris-1.0.0-incubating-bin.tar.gz
```

## Step 2: Configure Polaris

Navigate to the extracted directory:

```bash
cd apache-polaris-1.0.0-incubating-bin
```

Configure Polaris using environment variables or system properties. For example:

```bash
# Configure server port
export POLARIS_JAVA_OPTS="-Dquarkus.http.port=8080"

# Configure persistence type
export POLARIS_JAVA_OPTS="-Dpolaris.persistence.type=relational-jdbc"

# You can also combine multiple options
export POLARIS_JAVA_OPTS="-Xms512m -Xmx1g -Dquarkus.http.port=8080"
```

## Step 3: Start Polaris Server

Start the Polaris server:

```bash
bin/server
```

You should see output indicating that Polaris is starting up. When ready, you'll see messages similar to:

```
INFO  [io.quarkus] Apache Polaris Server (incubating) started in X.XXXs. Listening on: http://0.0.0.0:8181. Management interface listening on http://0.0.0.0:8182.
```

## Step 4: Verify the Installation

Verify that Polaris is running by checking the health endpoint:

```bash
curl http://localhost:8182/q/health
```

You should receive a response indicating the server is healthy.

## Step 5: Stop Polaris Server

To stop the Polaris server, press `Ctrl+C` in the terminal where it's running.

## Admin Tool

The binary distribution includes an admin tool for administrative and maintenance tasks:

```bash
bin/admin --help              # Show admin commands
bin/admin bootstrap -h        # Show bootstrap help
bin/admin purge -h            # Show purge help
```

## Next Steps

For instructions on building and running Polaris from source code, see the [Quickstart]({{% relref "quickstart" %}}) guide.

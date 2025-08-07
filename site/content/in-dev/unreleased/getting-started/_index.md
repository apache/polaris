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
title: 'Getting Started'
type: docs
weight: 101
build:
  render: never
---

# Getting Started with Apache Polaris Binary Distribution

Quickly start Apache Polaris by running the pre-built binary, no build needed.

---

## Prerequisites

- Java 21 or later installed. You can verify this by running:

```bash
java -version
```

- Basic familiarity with running commands in your operating system terminal.

---

## Step 1: Download the Apache Polaris Binary

1. Visit the official Apache Polaris GitHub
   [Releases page](https://github.com/apache/polaris/releases).

2. Download the latest binary archive file, for example:

```bash
apache-polaris-<version>-bin.tar.gz
```

---

## Step 2: Extract the Archive

Extract the downloaded tar.gz file to your desired directory:

```bash
tar -xzvf apache-polaris-<version>-bin.tar.gz
cd apache-polaris-<version>
```

*Replace `<version>` with the actual version number.*

---

## Step 3: Configure Polaris (Optional)

Edit the `application.properties` file if needed. For example:

```bash
polaris.storage.backend=local
polaris.storage.local.path=/data/polaris
```
*(This is a configuration file, not a shell command. Adjust these values as needed.)*

---

## Step 4: Run the Polaris Server

Start the Polaris server using the provided script:

```bash
./bin/polaris-server.sh start
```

To tail the logs in a separate terminal, run:

```bash
tail -f logs/polaris.log
```

---

## Step 5: Verify the Server is Running

Open your browser and navigate to:

```bash
curl http://localhost:8181/api/catalog/v1/health
```

You should see the Polaris server running or be able to access its REST API.

---

## Step 6: Stop the Polaris Server

To stop the server, run:

```bash
./bin/polaris-server.sh stop
```

---

## Additional Resources

- See the official Apache Polaris documentation for details.
- Use ./bin/polaris-admin in the binary for admin and maintenance tasks.

---

Get started with Apache Polaris binaries. See repo for containers and more.

---

*Happy data cataloging with Apache Polaris!*

---

**Want to build or run Polaris from source?**  
See the [Quickstart guide](https://polaris.apache.org/in-dev/unreleased/getting-started/quickstart/) for developer instructions and advanced setup.

---

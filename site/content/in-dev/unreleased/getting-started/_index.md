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

# Getting Started with Apache Polaris Binary Distribution

Quickly start Apache Polaris by running the pre-built binary, no build needed.

---

## Prerequisites

- Java 21 or later installed. You can verify this by running:

```bash
java -version
```

---

## Step 1: Download the Apache Polaris Binary

1. Visit the official Apache Polaris GitHub
   [Releases page](https://github.com/apache/polaris/releases).

2. Download the latest binary archive file, for example:

```bash
curl -L https://downloads.apache.org/incubator/polaris/1.0.0-incubating/polaris-bin-1.0.0-incubating.tgz | tar xz
```

---

## Step 2: Extract the Archive

Extract the downloaded tar.gz file to your desired directory:

```bash
cd apache-polaris-1.0.0-incubating-bin
```

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

- See the [official Apache Polaris documentation](https://polaris.apache.org/docs/) for comprehensive information on configuration, deployment, and usage.
- Use `./bin/polaris-admin` in the binary distribution for administrative and maintenance tasks.

---

## Other Getting Started Options

For building and running Polaris from source code, check out the [Quickstart guide](./quick-start/).

For Docker Compose examples and other deployment options, please see the [documentation](https://polaris.apache.org).

---

### Getting Help

- Documentation: https://polaris.apache.org
- GitHub Issues: https://github.com/apache/polaris/issues
- Slack: [Join Apache Polaris Community](https://join.slack.com/t/apache-polaris/shared_invite/zt-2y3l3r0fr-VtoW42ltir~nSzCYOrQgfw)

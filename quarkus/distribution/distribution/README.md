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

# Apache Polaris Distribution

This distribution contains both the Polaris Server and Admin Tool in a single package.

## Prerequisites

- Java SE 21 or higher

## Directory Structure

```
polaris-quarkus-distribution-@version@/
├── LICENSE
├── NOTICE
├── README.md
├── admin/           # Admin tool files
├── server/          # Server files
└── run.sh
```

## Usage

The `run.sh` script can launch either the server or admin tool:

### Start the Server

```bash
./run.sh server     # or just ./run.sh (server is default)
```

### Use the Admin Tool

```bash
./run.sh admin --help              # Show admin commands
./run.sh admin bootstrap -h        # Show bootstrap help
./run.sh admin purge -h           # Show purge help
```

### Configuration

Both components can be configured using environment variables or system properties. For example:

```bash
# Configure server port
JAVA_OPTS="-Dquarkus.http.port=8080" ./run.sh server

# Configure admin tool
JAVA_OPTS="-Dpolaris.persistence.type=relational-jdbc" ./run.sh admin
```

For more details on configuration, please refer to the Polaris documentation:
https://polaris.apache.org/ 
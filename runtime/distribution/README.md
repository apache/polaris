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

# Apache Polaris Distribution

This distribution contains both the Polaris Server and Admin Tool.

## Prerequisites

- Java SE 21 or higher

## Directory Structure

```
polaris-distribution-@version@/
├── LICENSE
├── NOTICE
├── README.md
├── admin/           # Admin tool files
│   └── config/
│       └── application.properties
├── bin/             # Executable scripts
│   ├── admin
│   └── server
└── server/          # Server files
    └── config/
        └── application.properties
```

## Usage

The distribution includes separate scripts for running the server and admin tool:

### Start the Server

```bash
bin/server
```

### Use the Admin Tool

```bash
bin/admin --help              # Show admin commands
bin/admin bootstrap -h        # Show bootstrap help
bin/admin purge -h            # Show purge help
```

For full usage instructions and configuration details, see the official Polaris docs at https://polaris.apache.org/.

### Configuration

Each component reads its own configuration file:

- `server/config/application.properties` for the server
- `admin/config/application.properties` for the admin tool

Both files are shipped with all properties commented out. Uncomment and edit the properties you want to change.

Both components can also be configured using environment variables or system properties, which take
precedence over the configuration files. For example:

```bash
# Configure server port
POLARIS_JAVA_OPTS="-Dquarkus.http.port=8080" bin/server

# Configure admin tool
POLARIS_JAVA_OPTS="-Dpolaris.persistence.type=relational-jdbc" bin/admin

# You can also set JAVA_OPTS as an environment variable
export POLARIS_JAVA_OPTS="-Xms512m -Xmx1g -Dquarkus.http.port=8080"
bin/server
```

For more details on configuration, please refer to the Polaris documentation:
https://polaris.apache.org/
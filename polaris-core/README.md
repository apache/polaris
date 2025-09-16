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

# Polaris-Core Module

The `polaris-core` module contains the fundamental entity definitions, core business logic, and foundational interfaces that power Apache Polaris.

## Overview

`polaris-core` handles:
- Entity Management
- Security & Access Control
- Common Persistence Logic
- Object Storage Integration

## Key Components

### Entity Management
Polaris has several different entities defined in `PolarisEntityType`. You can find their definitions in the package `org.apache.polaris.core.entity`.

### Security & Access Control
Polaris uses a role-based access control (RBAC) model. `PolarisPrivilege` defines the available privileges. The RBAC model is implemented in `PolarisAuthorizer`. For more information on authorization, see the package `org.apache.polaris.core.auth`.

### Common Persistence Logic
To store entities, Polaris provides a persistence layer built upon the `BasePersistence` interface. Common persistence logic is handled in the `org.apache.polaris.core.persistence` package. Implementations are found within [the persistence folder](../persistence).

### Object Storage Integration
Polaris supports multiple object storage providers. `PolarisStorageIntegration` provides an interface for accessing and managing data in object stores. For more information, see the package `org.apache.polaris.core.storage`.

## Usage

The `polaris-core` module is primarily consumed by:
- **API Services** - Management and catalog REST services
- **Runtime Components** - Server and admin tools
- **Persistence Implementations** - Eclipselink, JDBC, etc.

## Building

This module is built as part of the main Polaris build:

```bash
./gradlew :polaris-core:build
```

## Testing

Run the core module tests:

```bash
./gradlew :polaris-core:test
```
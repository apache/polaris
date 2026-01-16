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
title: External Policy Decision Point
linkTitle: External PDP
type: docs
weight: 300
---

This section provides information about integrating Apache Polaris (Incubating) with external Policy Decision Points (PDPs) for authorization.

## Overview

By default, Apache Polaris uses an internal authorization system based on role-based access control (RBAC). For advanced use cases requiring external policy management, centralized authorization, or integration with existing policy infrastructure, Polaris supports integration with external Policy Decision Points (PDPs).

## What is a Policy Decision Point?

A Policy Decision Point (PDP) is a component that evaluates authorization requests against defined policies and returns authorization decisions (allow/deny). 

Organizations may choose to use an external PDP instead of Polaris's internal authorization in order to leverage a centralized policy store that manages authorization policies across multiple services and applications.

## Architecture

When using an external PDP, Polaris delegates authorization decisions as follows:

1. **Client request**: A client makes a request to Polaris (e.g., read a table)
2. **Authorization check**: Polaris sends an authorization request to the external PDP
3. **Policy evaluation**: The PDP evaluates the request against configured policies
4. **Decision**: The PDP returns an allow/deny decision
5. **Enforcement**: Polaris enforces the decision and proceeds or rejects the request

```
┌─────────┐          ┌─────────────┐         ┌──────────────┐
│ Client  │─────────>│   Polaris   │────────>│ External PDP │
│         │ Request  │             │ AuthZ   │              │
│         │          │             │ Request │              │
│         │<─────────│             │<────────│              │
│         │ Response │             │ Decision│              │
└─────────┘          └─────────────┘         └──────────────┘
```

## Available Implementations

Apache Polaris currently supports the following external PDP integrations:

- **[Open Policy Agent (OPA)]({{< relref "opa.md" >}})**: A general-purpose policy engine with a rich ecosystem and flexible policy language (Rego)

## Configuration

To enable external PDP integration, set the following configuration property:

```properties
polaris.authorization.type=<pdp-type>
```

Where `<pdp-type>` is the identifier for the PDP implementation (e.g., `opa`). The default value is `internal`.

See the specific PDP documentation for detailed configuration options:

- [OPA Configuration]({{< relref "opa.md#configuration-reference" >}})

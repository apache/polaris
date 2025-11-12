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
title: Open Policy Agent (OPA) Integration
linkTitle: OPA
type: docs
weight: 100
---

{{% alert title="Experimental Feature" color="warning" %}}
**OPA integration is currently an experimental feature** and may undergo breaking changes in future versions. Use with caution in production environments.
{{% /alert %}}

This page describes how to integrate Apache Polaris (Incubating) with [Open Policy Agent (OPA)](https://www.openpolicyagent.org/) for external authorization.

## Overview

Open Policy Agent (OPA) is a general-purpose policy engine that enables unified, context-aware policy enforcement across your stack. OPA provides a high-level declarative language (Rego) for authoring policies and APIs to offload policy decision-making from your software.

Key benefits of using OPA with Polaris:

- **Flexible policy language**: Write authorization logic in Rego, a powerful declarative language
- **Centralized policy management**: Manage all policies in a single location
- **Policy testing**: Write unit tests for your authorization policies
- **Rich ecosystem**: Integrate with policy bundles, decision logs, and management tools
- **Attribute-based access control**: Make decisions based on user attributes, resource properties, and environmental context

## Prerequisites

Before configuring OPA integration:

1. **OPA Server**: Deploy and configure an OPA server accessible from Polaris
2. **Policy Definition**: Write and deploy authorization policies to OPA
3. **Network Access**: Ensure Polaris can reach the OPA server

## Quick Start

### 1. Deploy OPA

Deploy OPA server with your policies. For example, using Docker:

```bash
docker run -d \
  --name opa \
  -p 8181:8181 \
  -v $(pwd)/policies:/policies \
  openpolicyagent/opa:latest \
  run --server --addr :8181 /policies
```

### 2. Create a Policy

Create a policy file (e.g., `policies/polaris.rego`):

```rego
package polaris.authz

import future.keywords.if

# Default deny
default allow := false

# Allow admins to do everything
allow if {
    "ADMIN" in input.actor.roles
}

# Allow read operations on tables in analytics catalogs
allow if {
    input.action == "LOAD_TABLE_WITH_READ_DELEGATION"
    some target in input.resource.targets
    some parent in target.parents
    parent.type == "CATALOG"
    startswith(parent.name, "analytics_")
}
```

### 3. Configure Polaris

Add the following to your Polaris configuration:

```properties
# Enable OPA authorization
polaris.authorization.type=opa

# OPA server endpoint
polaris.authorization.opa.policy-uri=http://localhost:8181/v1/data/polaris/authz/allow
```

### 4. Restart Polaris

Restart the Polaris service to apply the configuration.

## Configuration Reference

### Basic Configuration

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `polaris.authorization.type` | Yes | `internal` | Set to `opa` to enable OPA authorization |
| `polaris.authorization.opa.policy-uri` | Yes | - | Full URI to the OPA policy decision endpoint (must be http or https) |

### HTTP Configuration

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `polaris.authorization.opa.http.timeout` | No | `PT2S` | HTTP request timeout (ISO-8601 duration format, e.g., `PT2S`, `PT10S`) |
| `polaris.authorization.opa.http.verify-ssl` | No | `true` | Whether to verify SSL certificates |
| `polaris.authorization.opa.http.trust-store-path` | No | - | Path to the trust store containing CA certificates |
| `polaris.authorization.opa.http.trust-store-password` | No | - | Password for the trust store |

### Authentication Configuration

OPA integration supports two authentication modes:

#### No Authentication (default)

No authentication configuration needed if OPA server doesn't require authentication:

```properties
polaris.authorization.opa.auth.type=none
```

#### Bearer Token Authentication

**Static Bearer Token:**

Use a static bearer token:

```properties
polaris.authorization.opa.auth.type=bearer
polaris.authorization.opa.auth.bearer.static-token.value=your-secret-token
```

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `polaris.authorization.opa.auth.type` | No | `none` | Set to `bearer` to enable bearer token authentication |
| `polaris.authorization.opa.auth.bearer.static-token.value` | Yes* | - | The bearer token value (*when using static token) |

**File-Based Bearer Token with Auto-Refresh:**

Use a bearer token from a file with automatic refresh (ideal for JWT tokens):

```properties
polaris.authorization.opa.auth.type=bearer
polaris.authorization.opa.auth.bearer.file-based.path=/var/secrets/token.txt
polaris.authorization.opa.auth.bearer.file-based.refresh-interval=PT5M
polaris.authorization.opa.auth.bearer.file-based.jwt-expiration-refresh=true
polaris.authorization.opa.auth.bearer.file-based.jwt-expiration-buffer=PT1M
```

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `polaris.authorization.opa.auth.type` | No | `none` | Set to `bearer` to enable bearer token authentication |
| `polaris.authorization.opa.auth.bearer.file-based.path` | Yes* | - | Path to file containing the bearer token (*when using file-based token) |
| `polaris.authorization.opa.auth.bearer.file-based.refresh-interval` | No | - | How often to refresh the token (ISO-8601 duration, e.g., `PT5M` for 5 minutes). If not set and JWT refresh is disabled, token won't be refreshed |
| `polaris.authorization.opa.auth.bearer.file-based.jwt-expiration-refresh` | No | `true` | Automatically detect JWT tokens and refresh based on expiration time |
| `polaris.authorization.opa.auth.bearer.file-based.jwt-expiration-buffer` | No | `PT1M` | Buffer time before JWT expiration to trigger refresh (ISO-8601 duration) |

**JWT Auto-Refresh**: When `jwt-expiration-refresh` is enabled (default), if the token file contains a valid JWT with an `exp` claim, Polaris will automatically refresh the token shortly before it expires based on the `jwt-expiration-buffer` setting.

## Policy Development

### Input Document Structure

Polaris sends the following input structure to OPA:

```json
{
  "actor": {
    "principal": "user@example.com",
    "roles": ["role1", "role2"]
  },
  "action": "LOAD_TABLE_WITH_READ_DELEGATION",
  "resource": {
    "targets": [
      {
        "type": "TABLE",
        "name": "my_table",
        "parents": [
          {
            "type": "CATALOG",
            "name": "my_catalog"
          },
          {
            "type": "NAMESPACE",
            "name": "schema1"
          }
        ]
      }
    ],
    "secondaries": []
  },
  "context": {
    "request_id": "uuid"
  }
}
```

#### Actor Object

| Field | Type | Description |
|-------|------|-------------|
| `principal` | string | The principal identifier (e.g., username, service account) |
| `roles` | array | Array of role names assigned to the principal |

#### Action Field

The `action` field contains the operation being attempted as a string value from the `PolarisAuthorizableOperation` enum.

For the complete list of available operations, see the [PolarisAuthorizableOperation enum](https://github.com/apache/polaris/blob/main/polaris-core/src/main/java/org/apache/polaris/core/auth/PolarisAuthorizableOperation.java) in the source code.

Common examples include:
- Table operations: `LOAD_TABLE_WITH_READ_DELEGATION`, `LOAD_TABLE_WITH_WRITE_DELEGATION`, `CREATE_TABLE_DIRECT`, `UPDATE_TABLE`, `DROP_TABLE_WITHOUT_PURGE`
- Catalog operations: `CREATE_CATALOG`, `UPDATE_CATALOG`, `DELETE_CATALOG`
- Namespace operations: `CREATE_NAMESPACE`, `UPDATE_NAMESPACE_PROPERTIES`, `DROP_NAMESPACE`

#### Resource Object

| Field | Type | Description |
|-------|------|-------------|
| `targets` | array | Array of target resource objects (primary resources being accessed) |
| `secondaries` | array | Array of secondary resource objects (related resources, if any) |

Each resource object in `targets` or `secondaries` has:

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Resource type (CATALOG, NAMESPACE, TABLE, VIEW, PRINCIPAL, CATALOG_ROLE, etc.) |
| `name` | string | Resource name |
| `parents` | array | Array of parent resource objects in the hierarchy (e.g., catalog and namespace for a table) |

Each parent object has:

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Parent resource type |
| `name` | string | Parent resource name |

#### Context Object

| Field | Type | Description |
|-------|------|-------------|
| `request_id` | string | UUID for correlating requests with logs |

### Policy Example

```rego
package polaris.authz

import future.keywords.if
import future.keywords.in

default allow := false

# Admin role can do anything
allow if {
    "ADMIN" in input.actor.roles
}

# Data engineers can create/read/update tables
allow if {
    "DATA_ENGINEER" in input.actor.roles
    input.action in ["CREATE_TABLE", "LOAD_TABLE_WITH_READ_DELEGATION", "UPDATE_TABLE"]
    some target in input.resource.targets
    target.type == "TABLE"
}

# Analysts can only read tables
allow if {
    "ANALYST" in input.actor.roles
    input.action == "LOAD_TABLE_WITH_READ_DELEGATION"
    some target in input.resource.targets
    target.type == "TABLE"
}
```

### Testing Policies

OPA supports policy testing using `opa test`. Create a test file (e.g., `polaris_test.rego`):

```rego
package polaris.authz

import future.keywords.if

test_admin_can_do_anything if {
    allow with input as {
        "actor": {"principal": "admin", "roles": ["ADMIN"]},
        "action": "DELETE_TABLE",
        "resource": {
            "targets": [{
                "type": "TABLE",
                "name": "sensitive_table",
                "parents": [{"type": "CATALOG", "name": "prod"}]
            }],
            "secondaries": []
        },
        "context": {"request_id": "test"}
    }
}

test_analyst_cannot_delete if {
    not allow with input as {
        "actor": {"principal": "analyst", "roles": ["ANALYST"]},
        "action": "DELETE_TABLE",
        "resource": {
            "targets": [{
                "type": "TABLE",
                "name": "table",
                "parents": [{"type": "CATALOG", "name": "prod"}]
            }],
            "secondaries": []
        },
        "context": {"request_id": "test"}
    }
}

test_analyst_can_read if {
    allow with input as {
        "actor": {"principal": "analyst", "roles": ["ANALYST"]},
        "action": "LOAD_TABLE_WITH_READ_DELEGATION",
        "resource": {
            "targets": [{
                "type": "TABLE",
                "name": "table",
                "parents": [{"type": "CATALOG", "name": "prod"}]
            }],
            "secondaries": []
        },
        "context": {"request_id": "test"}
    }
}
```

Run tests:

```bash
opa test policies/
```

## Additional Resources

- [OPA Documentation](https://www.openpolicyagent.org/docs/latest/)
- [Rego Language Guide](https://www.openpolicyagent.org/docs/latest/policy-language/)
- [OPA Policy Testing](https://www.openpolicyagent.org/docs/latest/policy-testing/)
- [OPA Bundles](https://www.openpolicyagent.org/docs/latest/management-bundles/)

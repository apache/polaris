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

# OPA Authorization Input Model

This package contains the authoritative model for OPA authorization requests in Polaris.

## Single Source of Truth

The Java classes in this package serve as the **single source of truth** for the OPA input structure. The JSON Schema can be generated from these classes, ensuring consistency between code and documentation.

## Generating the JSON Schema

Run the Gradle task to regenerate the schema:

```bash
./gradlew :polaris-extensions-auth-opa:generateOpaSchema
```

The schema will be generated at: `extensions/auth/opa/impl/opa-input-schema.json`

## Model Classes

### OpaRequest
Top-level wrapper sent to OPA containing the input.

### OpaAuthorizationInput
Complete authorization context with:
- `actor`: Who is making the request
- `action`: What they want to do
- `resource`: What they want to access
- `context`: Request metadata

### Actor
Principal information:
- `principal`: User/service identifier
- `roles`: List of assigned roles

### Resource
Resources involved in the operation:
- `targets`: Primary resources being accessed
- `secondaries`: Secondary resources (e.g., source in RENAME)

### ResourceEntity
Individual resource with hierarchical context:
- `type`: Entity type (CATALOG, NAMESPACE, TABLE, etc.)
- `name`: Entity name
- `parents`: Hierarchical path of parent entities

### Context
Request metadata:
- `request_id`: Unique correlation ID for logging

## Schema Evolution

When adding new fields:

1. Add field to appropriate model interface
2. Add Javadoc explaining the field
3. Regenerate schema: `./gradlew :polaris-extensions-auth-opa:generateOpaSchema`
4. Update OPA policies to handle new field
5. Update documentation

The schema generation ensures backward compatibility by making all new fields optional by default.

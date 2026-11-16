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

This writes two schemas:

- `extensions/auth/opa/opa-input-schema.json`: one authorization question
- `extensions/auth/opa/opa-batch-input-schema.json`: many questions for one actor in one call

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
- `realm`: Realm identifier (allows OPA policies to scope decisions by realm)

## Batch Model Classes

Used when `polaris.authorization.opa.batch-policy-uri` is configured. One batch call replaces many
single-question calls, which makes large lists faster. They reuse `Actor`, `Context` and `Resource`
from above.

### OpaBatchRequest
Top-level wrapper sent to the OPA batch rule containing the input.

### OpaBatchAuthorizationInput
Many questions for one actor:
- `actor`: Who is making the requests (sent once)
- `context`: Request metadata (sent once)
- `requests`: The list of questions

### OpaBatchQuery
One question in a batch:
- `id`: Identifies the question. The batch rule returns its answers keyed by this ID
- `action`: What the actor wants to do
- `resource`: What they want to access

## Schema Evolution

When adding new fields:

1. Add field to appropriate model interface
2. Add Javadoc explaining the field
3. Regenerate schema: `./gradlew :polaris-extensions-auth-opa:generateOpaSchema`
4. Update OPA policies to handle new field
5. Update documentation

The schema generation ensures backward compatibility by making all new fields optional by default.

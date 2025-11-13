
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

# OPA Input Schema Management

This document describes how the OPA authorization input schema is managed in Apache Polaris.

## Overview

The OPA input schema follows a **schema-as-code** approach where:

1. **Java model classes** (in `model/` package) are the single source of truth
2. **JSON Schema** is automatically generated from these classes
3. **CI validation** ensures the schema stays in sync with the code

## Developer Workflow

### Modifying the Schema

When you need to add/modify fields in the OPA input:

1. **Update the model classes** in `src/main/java/org/apache/polaris/extension/auth/opa/model/`
   ```java
   @PolarisImmutable
   public interface Actor {
     String principal();
     List<String> roles();
     // Add new field here
   }
   ```

2. **Regenerate the JSON Schema**
   ```bash
   ./gradlew :polaris-extensions-auth-opa:generateOpaSchema
   ```

3. **Commit both changes**
   - The updated Java files
   - The updated `opa-input-schema.json`

4. **CI will validate** that the schema matches the code

### CI Validation

The `validateOpaSchema` task automatically runs during `./gradlew check`:

```bash
./gradlew :polaris-extensions-auth-opa:check
```

This task:
1. Generates schema from current code to a temp file
2. Compares it with the committed `opa-input-schema.json`
3. **Fails the build** if they don't match

#### What happens if validation fails?

You'll see an error like:

```
❌ OPA Schema validation failed!

The committed opa-input-schema.json does not match the generated schema.
This means the schema is out of sync with the model classes.

To fix this, run:
  ./gradlew :polaris-extensions-auth-opa:generateOpaSchema

Then commit the updated opa-input-schema.json file.
```

Simply run the suggested command and commit the regenerated schema.

## Gradle Tasks

### `generateOpaSchema`
Generates the JSON Schema from model classes.

```bash
./gradlew :polaris-extensions-auth-opa:generateOpaSchema
```

**Output**: `extensions/auth/opa/impl/opa-input-schema.json`

### `validateOpaSchema`
Validates that committed schema matches the code.

```bash
./gradlew :polaris-extensions-auth-opa:validateOpaSchema
```

**Runs automatically** as part of `:check` task.

## For OPA Policy Developers

The generated `opa-input-schema.json` documents the structure of authorization requests sent from Polaris to OPA.

## Model Classes Reference

| Class | Purpose | Key Fields |
|-------|---------|------------|
| `OpaRequest` | Top-level wrapper | `input` |
| `OpaAuthorizationInput` | Complete auth context | `actor`, `action`, `resource`, `context` |
| `Actor` | Principal information | `principal`, `roles` |
| `Resource` | Resources being accessed | `targets`, `secondaries` |
| `ResourceEntity` | Individual resource | `type`, `name`, `parents` |
| `Context` | Request metadata | `request_id` |

See the [model package README](src/main/java/org/apache/polaris/extension/auth/opa/model/README.md) for detailed usage examples.

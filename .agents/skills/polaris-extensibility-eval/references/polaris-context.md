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
# Polaris context for eval workers

This reference is loaded into a worker prompt only when the task
declares `loads_polaris_context: true`. Tasks that probe pure
discoverability (e.g. evaluating an AGENTS.md edit) deliberately do
NOT load it — that's how we measure whether AGENTS.md alone is
enough.

## Build & test entry points

- Java 21 server, Java 17 client. Quarkus + CDI.
- `./gradlew --version` to verify the wrapper.
- `./gradlew compileAll` — compile-only sanity check.
- `./gradlew :polaris-core:test` — fastest meaningful test target.
- `./gradlew :polaris-core:test --tests "<FQN-or-glob>"` — single
  test class or method.
- `./gradlew :polaris-runtime-service:test` — Quarkus-bootstrapped
  service tests (slower).
- `./gradlew format` — Spotless / Google Java Format. Run before
  committing.
- `./gradlew :<module>:check` — module-scoped equivalent of full
  CI gate.
- ASF license header is required on every new committed source
  file. Pattern is in any existing source file (e.g.
  `polaris-core/src/main/java/.../auth/PolarisAuthorizerFactory.java`).

## Module map (essentials)

- `polaris-core/` — domain model, persistence interfaces,
  authorizer model, policy types, credential vending interfaces.
- `api/` — REST contracts (Iceberg service, management service).
- `runtime/service/` — Quarkus CDI wiring; this is where most
  pluggable defaults are bound.
- `runtime/server/` — main deployable Quarkus server.
- `persistence/relational-jdbc/`, `persistence/nosql/` —
  persistence implementations.
- `extensions/` — out-of-tree-shaped optional modules:
  `auth/{opa,ranger}`, `federation/{hadoop,hive,bigquery}`.
- `gradle/projects.main.properties` — registers Gradle
  subprojects; new modules MUST be added here.

## Extension surfaces & where they live

| Surface | Primary file | Add a new impl by |
|---------|--------------|--------------------|
| Authorizer | `polaris-core/.../auth/PolarisAuthorizerFactory.java` | impl under `extensions/auth/<name>/impl/` (see opa, ranger) |
| Federated catalog | `polaris-core/.../catalog/FederatedCatalogFactory.java` | impl under `extensions/federation/<name>/`, register in `gradle/projects.main.properties` |
| Connection credential | `polaris-core/.../credentials/connection/ConnectionCredentialVendor.java` | impl + CDI binding in `runtime/service/.../credentials/` |
| Storage credential | `polaris-core/.../storage/PolarisCredentialVendor.java` | similar shape |
| Policy type | `polaris-core/.../policy/PolicyType.java` + `PredefinedPolicyTypes.java` | add JSON schema under `polaris-core/src/main/resources/schemas/policies/system/<name>/` and register in `PredefinedPolicyTypes` |
| Persistence | `polaris-core/.../persistence/{PolarisMetaStoreManager,BasePersistence,transactional/TransactionalPersistence}.java` | mirror `persistence/relational-jdbc/` or `persistence/nosql/` |
| Event listener | `runtime/service/.../events/listeners/` and `runtime/service/.../events/PolarisEventDispatcher.java` | new class + CDI registration |
| Admin REST | `api/management-service/` (contract) + `runtime/service/.../admin/` (delegator) | OpenAPI spec change + delegator + impl |

## Authorization privilege addition recipe (canonical)

Files an agent must touch when adding a new privilege:

1. `polaris-core/src/main/java/org/apache/polaris/core/auth/PolarisAuthorizableOperation.java`
   — add the enum constant (alphabetical placement).
2. `polaris-core/src/main/java/org/apache/polaris/core/auth/PolarisPrivilege.java`
   (if separate) — register the privilege if it represents a new
   permission, not just a new op.
3. The privilege-semantics map (look for the existing pattern in the
   default authorizer) so the privilege resolves to the right
   permission set.
4. `polaris-core/src/test/java/org/apache/polaris/core/auth/` —
   add or extend a test asserting the new operation maps as expected.
5. If the privilege is exposed via management API: update the OpenAPI
   spec under `api/management-service/`.

## Predefined policy type addition recipe

1. New directory `polaris-core/src/main/resources/schemas/policies/system/<kebab-name>/`
   with `schema.json` mirroring the structure of an existing one
   (e.g. `snapshot-expiry`).
2. Register the new policy type in
   `polaris-core/src/main/java/org/apache/polaris/core/policy/PredefinedPolicyTypes.java`.
3. Extend `PredefinedPolicyTypes`-related tests.

## Federation extension recipe

1. New module dir `extensions/federation/<name>/` with
   `build.gradle.kts` mirroring an existing extension
   (e.g. `extensions/federation/bigquery/`).
2. Register the module in `gradle/projects.main.properties` AND in
   `settings.gradle.kts` if needed.
3. Implement `FederatedCatalogFactory` (or whatever current
   contract). The base contract lives in `polaris-core/.../catalog/`.

## Things that look like patterns but aren't

- Don't reach for `META-INF/services` ServiceLoader; Polaris is
  CDI-driven for almost everything. Use
  `@Alternative` + `@Priority` + `@Identifier` instead.
- Don't introduce a new `ThreadLocal` for request-scoped state;
  the project tried and reverted (PR #1000). Use the request-
  scoped `CallContext` holder pattern that already exists.
- Don't shade or relocate dependencies in new modules. AGENTS.md
  prohibits `*.shaded.*` / `*.relocated.*` imports.

## Test conventions

- AssertJ for assertions (not Hamcrest, not raw JUnit).
- Test classes named `<Class>Test` for unit, `<Class>IT` (or
  Quarkus test naming) for integration.
- Two test-method styles tolerated: `testXxx` and BDD
  `verbNounExpectation`.
- Spotless / Google Java Format: run `./gradlew format` before
  committing.

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// This module hosts integration tests that must run against a Polaris server assembled with extra
// extensions that are NOT part of the base :polaris-runtime-service runtime classpath (e.g. the
// OPA authorizer).

plugins {
  alias(libs.plugins.quarkus)
  id("org.kordamp.gradle.jandex")
  id("polaris-runtime")
}

dependencies {
  // The application under test: the full Polaris service plus the OPA authorizer extension.
  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation(project(":polaris-runtime-service"))
  runtimeOnly(project(":polaris-extensions-auth-opa"))

  // Integration-test dependencies. The `intTest` source set inherits `testImplementation` (see the
  // polaris-runtime convention plugin).
  testImplementation(enforcedPlatform(libs.quarkus.bom))
  testImplementation(project(":polaris-core"))
  testImplementation(project(":polaris-api-management-model"))
  testImplementation(project(":polaris-tests"))
  testImplementation(testFixtures(project(":polaris-runtime-service")))

  testImplementation("io.quarkus:quarkus-junit")
  testImplementation("io.rest-assured:rest-assured")
  testImplementation(libs.jakarta.ws.rs.api)

  // The base class org.apache.polaris.service.it.test.PolarisRestCatalogFileIntegrationTest extends
  // Iceberg's CatalogTests and uses RESTCatalog; :polaris-tests exposes these only as
  // implementation, so the Iceberg API/core jars (plus their :tests classifier) are needed here.
  testImplementation(platform(libs.iceberg.bom))
  testImplementation("org.apache.iceberg:iceberg-api")
  testImplementation("org.apache.iceberg:iceberg-core")
  testImplementation("org.apache.iceberg:iceberg-api:${libs.versions.iceberg.get()}:tests")
  testImplementation("org.apache.iceberg:iceberg-core:${libs.versions.iceberg.get()}:tests")

  // Test containers for the OIDC server and the OPA server used in the integration tests.
  testImplementation(platform(libs.testcontainers.bom))
  testImplementation(project(":polaris-keycloak-testcontainer"))
  testImplementation(project(":polaris-opa-testcontainer"))
}

tasks.named("javadoc") { dependsOn("jandex") }

tasks.withType(Test::class.java).configureEach {
  // Note: the test secrets are referenced in org.apache.polaris.service.it.ServerManager
  environment("POLARIS_BOOTSTRAP_CREDENTIALS", "POLARIS,test-admin,test-secret")
}

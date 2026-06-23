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

// This module deliberately does NOT use the OpenAPI generator. The OpenLineage
// event schema is a oneOf over RunEvent / JobEvent / DatasetEvent, which the
// jaxrs-resteasy generator collapses into a single class with every variant's
// required fields marked @NotNull -- making every real OpenLineage event fail
// bean validation. Marquez (the OpenLineage reference server) takes the same
// approach: hand-written JAX-RS resource on top of the official
// `io.openlineage:openlineage-java` types, using Jackson polymorphism keyed on
// the `schemaURL` field to dispatch between event variants.
plugins {
  id("polaris-client")
  id("org.kordamp.gradle.jandex")
}

dependencies {
  implementation(project(":polaris-core"))

  // The official OpenLineage Java library ships POJOs for the receive side
  // (`io.openlineage.server.OpenLineage.{BaseEvent,RunEvent,JobEvent,DatasetEvent}`).
  // We expose them through the API surface; callers reading event content read
  // these types directly.
  api(libs.openlineage.java)

  implementation(libs.jakarta.annotation.api)
  implementation(libs.jakarta.inject.api)
  implementation(libs.jakarta.validation.api)

  implementation(libs.jakarta.servlet.api)
  implementation(libs.jakarta.ws.rs.api)

  implementation(platform(libs.micrometer.bom))
  implementation("io.micrometer:micrometer-core")

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")

  implementation(libs.slf4j.api)

  compileOnly(libs.microprofile.fault.tolerance.api)
}

tasks.named("javadoc") { dependsOn("jandex") }

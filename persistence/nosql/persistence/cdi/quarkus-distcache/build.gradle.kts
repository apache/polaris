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

plugins {
  id("org.kordamp.gradle.jandex")
  id("polaris-server")
}

description = "Polaris NoSQL persistence, distributed cache invalidation for Quarkus."

dependencies {
  implementation(project(":polaris-persistence-nosql-cdi-common"))
  implementation(project(":polaris-persistence-nosql-api"))
  implementation(project(":polaris-idgen-api"))
  runtimeOnly(project(":polaris-nodes-impl"))
  runtimeOnly(project(":polaris-nodes-store-nosql"))
  runtimeOnly(project(":polaris-realms-impl"))
  runtimeOnly(project(":polaris-realms-store-nosql"))
  runtimeOnly(project(":polaris-async-vertx"))
  runtimeOnly(project(":polaris-idgen-impl"))
  runtimeOnly(project(":polaris-authz-impl"))
  runtimeOnly(project(":polaris-authz-store-nosql"))

  compileOnly(platform(libs.micrometer.bom))
  compileOnly("io.micrometer:micrometer-core")
  compileOnly(platform(libs.opentelemetry.instrumentation.bom.alpha))
  compileOnly("io.opentelemetry.instrumentation:opentelemetry-instrumentation-annotations")

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-databind")

  compileOnly(libs.smallrye.config.core)

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  implementation(platform(libs.quarkus.bom))
  implementation("io.quarkus:quarkus-core")
  implementation("io.quarkus:quarkus-micrometer")
  implementation("io.quarkus:quarkus-mongodb-client")

  implementation(libs.jakarta.ws.rs.api)

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  // Must stick with the Quarkus platform versions of Vert.X
  // (signature of io.vertx.core.Vertx.createHttpClient() changed from 4.5 to 5.0)
  testImplementation(enforcedPlatform(libs.quarkus.bom))
}

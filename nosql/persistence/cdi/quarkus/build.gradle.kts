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
  alias(libs.plugins.jandex)
  id("polaris-server")
}

description = "Polaris persistence, providers for Quarkus."

dependencies {
  implementation(project(":polaris-persistence-cdi-common"))
  implementation(project(":polaris-persistence-api"))
  implementation(project(":polaris-persistence-impl"))
  implementation(project(":polaris-persistence-inmemory"))
  implementation(project(":polaris-persistence-mongodb"))
  implementation(project(":polaris-idgen-api"))
  runtimeOnly(project(":polaris-nodes-impl"))
  runtimeOnly(project(":polaris-nodes-store"))
  runtimeOnly(project(":polaris-realms-impl"))
  runtimeOnly(project(":polaris-realms-store"))
  runtimeOnly(project(":polaris-async-vertx"))
  runtimeOnly(project(":polaris-idgen-impl"))
  runtimeOnly(project(":polaris-authz-impl"))
  runtimeOnly(project(":polaris-authz-store"))

  compileOnly(platform(libs.micrometer.bom))
  compileOnly("io.micrometer:micrometer-core")
  compileOnly(platform(libs.opentelemetry.instrumentation.bom.alpha))
  compileOnly("io.opentelemetry.instrumentation:opentelemetry-instrumentation-annotations")

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.fasterxml.jackson.core:jackson-databind")

  compileOnly(libs.smallrye.config.core)

  implementation(platform(libs.quarkus.bom))
  implementation("io.quarkus:quarkus-core")
  implementation("io.quarkus:quarkus-micrometer")
  implementation("io.quarkus:quarkus-mongodb-client")

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))
}

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

description = "Polaris NoSQL persistence, in-memory implementation"

dependencies {
  implementation(project(":polaris-persistence-nosql-api"))
  implementation(project(":polaris-persistence-nosql-impl"))
  implementation(project(":polaris-idgen-api"))

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  implementation(platform(libs.micrometer.bom))
  implementation("io.micrometer:micrometer-core")
  compileOnly(platform(libs.opentelemetry.instrumentation.bom.alpha))
  compileOnly("io.opentelemetry.instrumentation:opentelemetry-instrumentation-annotations")

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.smallrye.config.core)

  compileOnly(platform(libs.quarkus.bom))
  compileOnly("io.quarkus:quarkus-core")

  testFixturesCompileOnly(platform(libs.jackson.bom))
  testFixturesCompileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testFixturesCompileOnly(libs.jakarta.annotation.api)
  testFixturesCompileOnly(libs.jakarta.validation.api)

  testFixturesCompileOnly(project(":polaris-immutables"))
  testFixturesAnnotationProcessor(project(":polaris-immutables", configuration = "processor"))

  testFixturesApi(testFixtures(project(":polaris-persistence-nosql-impl")))
  testFixturesApi(project(":polaris-persistence-nosql-testextension"))
}

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

description = "Polaris NoSQL persistence maintenance - service implementation"

dependencies {
  implementation(project(":polaris-persistence-nosql-api"))
  implementation(project(":polaris-persistence-nosql-maintenance-api"))
  implementation(project(":polaris-persistence-nosql-maintenance-spi"))
  implementation(project(":polaris-persistence-nosql-realms-api"))
  implementation(project(":polaris-idgen-api"))
  runtimeOnly(project(":polaris-persistence-nosql-realms-impl"))
  runtimeOnly(project(":polaris-persistence-nosql-realms-store-nosql"))

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.fasterxml.jackson.core:jackson-databind")

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  testFixturesApi(project(":polaris-persistence-nosql-api"))
  testFixturesApi(project(":polaris-persistence-nosql-maintenance-api"))
  testFixturesApi(project(":polaris-persistence-nosql-maintenance-spi"))
  testFixturesApi(project(":polaris-persistence-nosql-testextension"))

  testFixturesCompileOnly(project(":polaris-immutables"))
  testFixturesAnnotationProcessor(project(":polaris-immutables", configuration = "processor"))

  testFixturesCompileOnly(platform(libs.jackson.bom))
  testFixturesCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
  testFixturesCompileOnly("com.fasterxml.jackson.core:jackson-databind")

  testFixturesImplementation(libs.jakarta.annotation.api)
  testFixturesImplementation(libs.jakarta.validation.api)
  testFixturesCompileOnly(libs.jakarta.enterprise.cdi.api)

  testCompileOnly(platform(libs.jackson.bom))
  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
  testCompileOnly("com.fasterxml.jackson.core:jackson-databind")

  testRuntimeOnly(libs.logback.classic)

  testImplementation(project(":polaris-idgen-mocks"))
  testRuntimeOnly(testFixtures(project(":polaris-persistence-nosql-cdi-weld")))
  testImplementation(libs.weld.se.core)
  testImplementation(libs.weld.junit5)
  testRuntimeOnly(libs.smallrye.jandex)

  testRuntimeOnly(project(":polaris-persistence-nosql-realms-impl"))
  testRuntimeOnly(project(":polaris-persistence-nosql-realms-store-nosql"))
  testRuntimeOnly(project(":polaris-persistence-nosql-inmemory"))
  testImplementation(testFixtures(project(":polaris-persistence-nosql-inmemory")))
}

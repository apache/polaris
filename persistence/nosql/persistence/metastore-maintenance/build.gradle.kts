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

description = "Polaris NoSQL persistence core types"

dependencies {
  implementation(project(":polaris-core"))
  implementation(project(":polaris-persistence-nosql-api"))
  implementation(project(":polaris-idgen-api"))
  implementation(project(":polaris-persistence-nosql-maintenance-api"))
  implementation(project(":polaris-persistence-nosql-maintenance-spi"))
  implementation(project(":polaris-persistence-nosql-maintenance-cel"))
  implementation(project(":polaris-persistence-nosql-metastore-types"))

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-smile")

  compileOnly(libs.smallrye.config.core)
  compileOnly(platform(libs.quarkus.bom))
  compileOnly("io.quarkus:quarkus-core")

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)

  testFixturesImplementation(project(":polaris-core"))
  testFixturesImplementation(project(":polaris-persistence-nosql-metastore"))
  testFixturesImplementation(testFixtures(project(":polaris-persistence-nosql-metastore")))
  testRuntimeOnly(project(":polaris-persistence-nosql-authz-impl"))
  testRuntimeOnly(project(":polaris-persistence-nosql-authz-store-nosql"))

  testFixturesImplementation(libs.jakarta.annotation.api)
  testFixturesImplementation(libs.jakarta.validation.api)
  testFixturesImplementation(libs.jakarta.enterprise.cdi.api)
  testImplementation(libs.smallrye.common.annotation)

  testFixturesImplementation(platform(libs.jackson.bom))
  testFixturesImplementation("com.fasterxml.jackson.core:jackson-annotations")
  testFixturesImplementation("com.fasterxml.jackson.core:jackson-core")

  testImplementation(project(":polaris-idgen-mocks"))
  testImplementation(testFixtures(project(":polaris-persistence-nosql-maintenance-impl")))
  testImplementation(project(":polaris-persistence-nosql-impl"))

  testRuntimeOnly(testFixtures(project(":polaris-persistence-nosql-cdi-weld")))
  testImplementation(libs.weld.se.core)
  testImplementation(libs.weld.junit5)
  testRuntimeOnly(libs.smallrye.jandex)
}

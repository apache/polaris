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

description = "Polaris nodes NoSQL persistence"

dependencies {
  implementation(project(":polaris-nodes-api"))
  implementation(project(":polaris-nodes-spi"))
  implementation(project(":polaris-idgen-api"))
  implementation(project(":polaris-persistence-nosql-api"))
  implementation(project(":polaris-persistence-nosql-maintenance-spi"))

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")
  runtimeOnly("com.fasterxml.jackson.datatype:jackson-datatype-guava")
  runtimeOnly("com.fasterxml.jackson.datatype:jackson-datatype-jdk8")
  runtimeOnly("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)

  testFixturesRuntimeOnly(project(":polaris-persistence-nosql-cdi-weld"))
  testFixturesApi(testFixtures(project(":polaris-persistence-nosql-cdi-weld")))

  testFixturesApi(libs.weld.se.core)
  testFixturesApi(libs.weld.junit5)
  testFixturesRuntimeOnly(libs.smallrye.jandex)

  testImplementation(project(":polaris-idgen-impl"))
  testImplementation(testFixtures(project(":polaris-persistence-nosql-inmemory")))
  testImplementation(testFixtures(project(":polaris-nodes-impl")))
  testImplementation(libs.threeten.extra)

  testCompileOnly(libs.jakarta.annotation.api)
  testCompileOnly(libs.jakarta.validation.api)
  testCompileOnly(libs.jakarta.inject.api)
  testCompileOnly(libs.jakarta.enterprise.cdi.api)
}

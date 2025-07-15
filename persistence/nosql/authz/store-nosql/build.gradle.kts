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

description = "Polaris AuthZ NoSQL persistence"

dependencies {
  implementation(project(":polaris-persistence-nosql-authz-api"))
  implementation(project(":polaris-persistence-nosql-authz-spi"))
  implementation(project(":polaris-persistence-nosql-api"))
  implementation(project(":polaris-persistence-nosql-maintenance-api"))
  implementation(project(":polaris-persistence-nosql-maintenance-spi"))

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)

  testFixturesApi(libs.weld.se.core)
  testFixturesApi(libs.weld.junit5)
  testRuntimeOnly(libs.smallrye.jandex)

  testFixturesRuntimeOnly(project(":polaris-persistence-nosql-cdi-weld"))
  testFixturesApi(testFixtures(project(":polaris-persistence-nosql-cdi-weld")))

  testImplementation(testFixtures(project(":polaris-persistence-nosql-inmemory")))
  testImplementation(libs.threeten.extra)

  testCompileOnly(libs.jakarta.annotation.api)
  testCompileOnly(libs.jakarta.validation.api)
  testCompileOnly(libs.jakarta.inject.api)
  testCompileOnly(libs.jakarta.enterprise.cdi.api)
}

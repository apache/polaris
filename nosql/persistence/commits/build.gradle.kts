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

description = "Polaris consistent and atomic commits"

dependencies {
  implementation(project(":polaris-persistence-api"))
  implementation(project(":polaris-idgen-api"))

  compileOnly(libs.smallrye.config.core)
  compileOnly(platform(libs.quarkus.bom))
  compileOnly("io.quarkus:quarkus-core")

  implementation(libs.guava)
  implementation(libs.agrona)
  implementation(libs.slf4j.api)

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)

  testFixturesApi(libs.guava)
  testFixturesApi(project(":polaris-persistence-api"))
  testFixturesApi(project(":polaris-persistence-testextension"))
  testFixturesApi(testFixtures(project(":polaris-persistence-api")))

  testFixturesCompileOnly(project(":polaris-immutables"))
  testFixturesAnnotationProcessor(project(":polaris-immutables", configuration = "processor"))

  testFixturesCompileOnly(libs.jakarta.annotation.api)
  testFixturesCompileOnly(libs.jakarta.validation.api)
  testFixturesCompileOnly(libs.jakarta.inject.api)

  testFixturesImplementation(platform(libs.jackson.bom))
  testFixturesImplementation("com.fasterxml.jackson.core:jackson-annotations")
  testFixturesImplementation("com.fasterxml.jackson.core:jackson-core")
  testFixturesImplementation("com.fasterxml.jackson.core:jackson-databind")
  testFixturesRuntimeOnly("com.fasterxml.jackson.datatype:jackson-datatype-guava")
  testFixturesRuntimeOnly("com.fasterxml.jackson.datatype:jackson-datatype-jdk8")
  testFixturesRuntimeOnly("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

  testImplementation(testFixtures(project(":polaris-persistence-inmemory")))
  testImplementation(project(":polaris-idgen-impl"))
}

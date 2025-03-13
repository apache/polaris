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

description = "Polaris core persistence base implementation"

dependencies {
  implementation(project(":polaris-persistence-api"))
  implementation(project(":polaris-persistence-delegate"))
  implementation(project(":polaris-idgen-api"))
  implementation(project(":polaris-idgen-spi"))

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-smile")

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)

  testFixturesApi(project(":polaris-persistence-api"))
  testFixturesApi(testFixtures(project(":polaris-persistence-api")))
  testFixturesApi(project(":polaris-persistence-testextension"))

  testFixturesCompileOnly(platform(libs.jackson.bom))
  testFixturesCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
  testFixturesCompileOnly("com.fasterxml.jackson.core:jackson-core")
  testFixturesCompileOnly("com.fasterxml.jackson.core:jackson-databind")

  testFixturesCompileOnly(libs.jakarta.annotation.api)
  testFixturesCompileOnly(libs.jakarta.validation.api)

  testFixturesCompileOnly(project(":polaris-immutables"))
  testFixturesAnnotationProcessor(project(":polaris-immutables", configuration = "processor"))

  testImplementation(libs.threeten.extra)
  testImplementation(testFixtures(project(":polaris-persistence-inmemory")))
}

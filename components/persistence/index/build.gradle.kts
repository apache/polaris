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
  alias(libs.plugins.jmh)
  id("polaris-server")
}

description = "Polaris index implementation"

dependencies {
  implementation(project(":polaris-persistence-api"))
  implementation(project(":polaris-persistence-varint"))

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  compileOnly(libs.smallrye.config.core)
  compileOnly(platform(libs.quarkus.bom))
  compileOnly("io.quarkus:quarkus-core")

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)

  testRuntimeOnly(platform(libs.jackson.bom))
  testRuntimeOnly("com.fasterxml.jackson.dataformat:jackson-dataformat-smile")
  testRuntimeOnly(testFixtures(project(":polaris-persistence-inmemory")))

  testFixturesApi(testFixtures(project(":polaris-persistence-api")))
  testFixturesApi(project(":polaris-persistence-testextension"))

  testFixturesCompileOnly(platform(libs.jackson.bom))
  testFixturesCompileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testFixturesCompileOnly(project(":polaris-immutables"))
  testFixturesAnnotationProcessor(project(":polaris-immutables", configuration = "processor"))

  testFixturesImplementation(libs.guava)

  testCompileOnly(libs.jakarta.annotation.api)
  testCompileOnly(libs.jakarta.validation.api)

  jmhImplementation(libs.jmh.core)
  jmhAnnotationProcessor(libs.jmh.generator.annprocess)
}

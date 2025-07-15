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

description = "Polaris nodes management implementation"

dependencies {
  implementation(project(":polaris-nodes-api"))
  implementation(project(":polaris-nodes-spi"))
  implementation(project(":polaris-idgen-api"))
  implementation(project(":polaris-idgen-spi"))
  implementation(project(":polaris-async-api"))

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)

  testFixturesApi(project(":polaris-idgen-api"))
  testFixturesApi(project(":polaris-nodes-api"))
  testFixturesApi(project(":polaris-nodes-spi"))

  testFixturesRuntimeOnly(libs.smallrye.jandex)

  testFixturesApi(platform(libs.jackson.bom))
  testFixturesApi("com.fasterxml.jackson.core:jackson-annotations")

  testImplementation(project(":polaris-idgen-mocks"))
  testImplementation(project(":polaris-async-java"))
  testImplementation(testFixtures(project(":polaris-async-api")))

  testFixturesApi(libs.jakarta.annotation.api)
}

tasks.withType<Javadoc> { isFailOnError = false }

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

description = "Polaris async execution - Vert.x"

dependencies {
  implementation(project(":polaris-async-api"))

  implementation(libs.slf4j.api)
  implementation(libs.guava)

  compileOnly(enforcedPlatform(libs.quarkus.bom))
  compileOnly("io.vertx:vertx-core")

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-databind")

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)

  testFixturesApi(libs.jakarta.annotation.api)
  testFixturesApi(libs.jakarta.validation.api)
  testFixturesApi(libs.jakarta.inject.api)
  testFixturesApi(libs.jakarta.enterprise.cdi.api)

  testFixturesApi(enforcedPlatform(libs.quarkus.bom))
  testFixturesApi("io.vertx:vertx-core")

  testImplementation(testFixtures(project(":polaris-async-api")))
}

tasks.withType<Javadoc> {
  isFailOnError = false
  options.memberLevel = JavadocMemberLevel.PACKAGE
}

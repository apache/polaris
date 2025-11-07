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

description =
  "Polaris NoSQL persistence JUnit test suite to use Polaris NoSQL persistence in tests, no production code."

dependencies {
  implementation(project(":polaris-persistence-nosql-api"))
  implementation(project(":polaris-persistence-nosql-impl"))
  implementation(project(":polaris-idgen-api"))
  implementation(project(":polaris-idgen-impl"))
  implementation(project(":polaris-idgen-spi"))

  implementation(platform(libs.micrometer.bom))
  implementation("io.micrometer:micrometer-core")

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  implementation(platform(libs.junit.bom))
  implementation("org.junit.jupiter:junit-jupiter")
}

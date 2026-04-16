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
  id("polaris-server")
  id("org.kordamp.gradle.jandex")
}

dependencies {
  implementation(project(":polaris-core"))
  implementation(project(":polaris-api-metrics-reports-service"))

  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")

  implementation(libs.jakarta.enterprise.cdi.api)
  implementation(libs.jakarta.inject.api)
  implementation(libs.jakarta.ws.rs.api)
  compileOnly(libs.jakarta.annotation.api)

  testImplementation(platform(libs.junit.bom))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation(libs.assertj.core)
  testImplementation(libs.mockito.core)
  testImplementation(libs.jakarta.ws.rs.api)

  // Provides jakarta.ws.rs.ext.RuntimeDelegate needed to build Response objects in unit tests
  testRuntimeOnly(enforcedPlatform(libs.quarkus.bom))
  testRuntimeOnly("io.quarkus.resteasy.reactive:resteasy-reactive")
}

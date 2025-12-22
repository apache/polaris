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

description = "Polaris NoSQL persistence - bridge to meta-store"

dependencies {
  implementation(project(":polaris-core"))

  implementation(project(":polaris-persistence-nosql-api"))
  implementation(project(":polaris-persistence-nosql-metastore-types"))
  implementation(project(":polaris-persistence-nosql-authz-api"))
  implementation(project(":polaris-persistence-nosql-authz-spi"))
  implementation(project(":polaris-persistence-nosql-realms-api"))
  implementation(project(":polaris-idgen-api"))
  implementation(project(":polaris-api-catalog-service"))

  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.smallrye.common.annotation)

  testImplementation(testFixtures(project(":polaris-core")))
  testRuntimeOnly(project(":polaris-persistence-nosql-authz-impl"))
  testRuntimeOnly(project(":polaris-persistence-nosql-authz-store-nosql"))
  testRuntimeOnly(testFixtures(project(":polaris-persistence-nosql-cdi-weld")))
  testImplementation(libs.weld.se.core)
  testImplementation(libs.weld.junit5)
  testRuntimeOnly(libs.smallrye.jandex)

  testFixturesImplementation(project(":polaris-core"))
  testFixturesImplementation(testFixtures(project(":polaris-core")))
  testFixturesImplementation(libs.jakarta.annotation.api)
  testFixturesImplementation(libs.jakarta.validation.api)
  testFixturesImplementation(libs.jakarta.enterprise.cdi.api)
  testCompileOnly(libs.smallrye.common.annotation)
}

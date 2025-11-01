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

description = "Polaris NoSQL persistence, providers for CDI/Weld."

dependencies {
  implementation(project(":polaris-persistence-nosql-cdi-common"))
  implementation(project(":polaris-persistence-nosql-api"))
  runtimeOnly(project(":polaris-nodes-impl"))
  runtimeOnly(project(":polaris-nodes-store-nosql"))
  runtimeOnly(project(":polaris-realms-impl"))
  runtimeOnly(project(":polaris-realms-store-nosql"))

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.fasterxml.jackson.core:jackson-databind")

  compileOnly(libs.smallrye.config.core)
  compileOnly(platform(libs.quarkus.bom))
  compileOnly("io.quarkus:quarkus-core")

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  testFixturesApi(project(":polaris-persistence-nosql-api"))
  testFixturesApi(project(":polaris-realms-api"))
  testFixturesApi(platform(libs.jackson.bom))
  testFixturesApi("com.fasterxml.jackson.dataformat:jackson-dataformat-smile")
  testFixturesApi(project(":polaris-persistence-nosql-inmemory"))
  testFixturesApi(testFixtures(project(":polaris-persistence-nosql-inmemory")))
  testFixturesImplementation(project(":polaris-persistence-nosql-cdi-common"))
  testFixturesImplementation(project(":polaris-async-api"))
  testFixturesRuntimeOnly(project(":polaris-async-java"))
  testFixturesApi(libs.jakarta.inject.api)
  testFixturesApi(libs.jakarta.enterprise.cdi.api)
  testFixturesApi(project(":polaris-idgen-api"))
  testFixturesApi(project(":polaris-nodes-api"))
  testFixturesRuntimeOnly(libs.smallrye.config.core)

  testImplementation(libs.weld.se.core)
  testImplementation(libs.weld.junit5)
  testRuntimeOnly(libs.smallrye.jandex)
}

tasks.withType<Javadoc> { isFailOnError = false }

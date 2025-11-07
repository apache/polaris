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

description = "Polaris NoSQL persistence API, no concrete implementations"

dependencies {
  api(project(":polaris-version"))
  api(project(":polaris-misc-types"))

  implementation(project(":polaris-idgen-api"))
  implementation(project(":polaris-nodes-api"))
  implementation(project(":polaris-persistence-nosql-varint"))

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")
  runtimeOnly("com.fasterxml.jackson.datatype:jackson-datatype-guava")
  runtimeOnly("com.fasterxml.jackson.datatype:jackson-datatype-jdk8")
  runtimeOnly("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  compileOnly(libs.smallrye.config.core)
  compileOnly(platform(libs.quarkus.bom))
  compileOnly("io.quarkus:quarkus-core")

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)

  testImplementation(platform(libs.jackson.bom))
  testImplementation("com.fasterxml.jackson.dataformat:jackson-dataformat-smile")

  testImplementation(libs.junit.pioneer)

  testFixturesImplementation(platform(libs.jackson.bom))
  testFixturesImplementation("com.fasterxml.jackson.core:jackson-databind")

  testFixturesCompileOnly(project(":polaris-immutables"))
  testFixturesAnnotationProcessor(project(":polaris-immutables", configuration = "processor"))

  testFixturesCompileOnly(libs.jakarta.annotation.api)
  testFixturesCompileOnly(libs.jakarta.validation.api)
}

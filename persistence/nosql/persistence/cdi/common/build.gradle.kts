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

description = "Polaris NoSQL persistence, providers for CDI (not Quarkus)."

dependencies {
  implementation(project(":polaris-persistence-nosql-api"))
  implementation(project(":polaris-realms-api"))
  implementation(project(":polaris-async-api"))
  implementation(project(":polaris-idgen-api"))
  implementation(project(":polaris-nodes-api"))
  runtimeOnly(project(":polaris-nodes-impl"))
  runtimeOnly(project(":polaris-nodes-store-nosql"))
  runtimeOnly(project(":polaris-realms-impl"))
  runtimeOnly(project(":polaris-realms-store-nosql"))

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.fasterxml.jackson.core:jackson-databind")

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))
}

tasks.withType<Javadoc> { isFailOnError = false }

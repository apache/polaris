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
  // Jakarta APIs
  compileOnly(libs.jakarta.ws.rs.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.inject.api)

  // Jersey HTTP server
  implementation(platform(libs.jersey.bom))
  implementation("org.glassfish.jersey.core:jersey-server")
  implementation("org.glassfish.jersey.containers:jersey-container-servlet")
  implementation("org.glassfish.jersey.inject:jersey-hk2")
  implementation("org.glassfish.jersey.media:jersey-media-json-jackson")

  // Jetty HTTP server (for jersey-container-jetty-http)
  implementation(platform(libs.jetty.bom))
  implementation("org.glassfish.jersey.containers:jersey-container-jetty-http")
  implementation("org.eclipse.jetty:jetty-server")
  implementation("org.eclipse.jetty:jetty-util")

  // Jackson
  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-xml")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
  implementation("com.fasterxml.jackson.jakarta.rs:jackson-jakarta-rs-json-provider")
  implementation("com.fasterxml.jackson.jakarta.rs:jackson-jakarta-rs-xml-provider")

  // Immutables
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  // Other
  implementation(libs.guava)
  implementation(libs.slf4j.api)
}

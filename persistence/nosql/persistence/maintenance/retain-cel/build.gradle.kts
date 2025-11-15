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
  alias(libs.plugins.jmh)
  id("polaris-server")
}

description = "Polaris NoSQL persistence maintenance - reference retain check using CEL"

dependencies {
  implementation(project(":polaris-persistence-nosql-api"))
  implementation(platform(libs.cel.bom))
  implementation("org.projectnessie.cel:cel-standalone")
  implementation(libs.caffeine)

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testCompileOnly(platform(libs.jackson.bom))
  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")

  jmhImplementation(libs.jmh.core)
  jmhAnnotationProcessor(libs.jmh.generator.annprocess)
}

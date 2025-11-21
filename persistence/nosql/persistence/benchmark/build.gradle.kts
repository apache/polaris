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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  id("polaris-server")
  id("com.gradleup.shadow")
  alias(libs.plugins.jmh)
}

description = "Polaris NoSQL persistence benchmarks, no production code"

dependencies {
  implementation(project(":polaris-persistence-nosql-api"))
  implementation(project(":polaris-persistence-nosql-impl"))
  implementation(project(":polaris-persistence-nosql-standalone"))
  implementation(project(":polaris-idgen-api"))
  implementation(project(":polaris-idgen-impl"))
  implementation(project(":polaris-idgen-spi"))

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.fasterxml.jackson.core:jackson-databind")

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)

  jmhImplementation(libs.jmh.core)
  jmhAnnotationProcessor(libs.jmh.generator.annprocess)

  jmhRuntimeOnly(project(":polaris-persistence-nosql-inmemory"))
  jmhRuntimeOnly(testFixtures(project(":polaris-persistence-nosql-inmemory")))

  jmhRuntimeOnly(project(":polaris-persistence-nosql-mongodb"))
  jmhRuntimeOnly(testFixtures(project(":polaris-persistence-nosql-mongodb")))
}

tasks.named<ShadowJar>("jmhJar").configure {
  outputs.cacheIf { false } // do not cache uber/shaded jars
  mergeServiceFiles()
  isZip64 = true
}

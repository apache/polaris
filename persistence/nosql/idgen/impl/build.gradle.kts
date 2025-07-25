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
  alias(libs.plugins.jandex)
  alias(libs.plugins.jmh)
  alias(libs.plugins.jcstress)
  id("polaris-server")
}

description = "Polaris ID generation implementation"

dependencies {
  implementation(project(":polaris-idgen-api"))
  implementation(project(":polaris-idgen-spi"))

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)

  compileOnly(libs.smallrye.config.core)
  compileOnly(platform(libs.quarkus.bom))
  compileOnly("io.quarkus:quarkus-core")

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")

  testFixturesCompileOnly(platform(libs.jackson.bom))
  testFixturesCompileOnly("com.fasterxml.jackson.core:jackson-databind")

  testFixturesCompileOnly(libs.jakarta.inject.api)
  testFixturesCompileOnly(libs.jakarta.enterprise.cdi.api)

  testImplementation(project(":polaris-idgen-mocks"))

  jmhImplementation(libs.jmh.core)
  jmhAnnotationProcessor(libs.jmh.generator.annprocess)
}

tasks.named("jcstressJar") { dependsOn("jandex") }

tasks.named("compileJcstressJava") { dependsOn("jandex") }

tasks.named("check") { dependsOn("jcstress") }

jcstress { jcstressDependency = libs.jcstress.core.get().toString() }

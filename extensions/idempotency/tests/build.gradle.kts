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
  alias(libs.plugins.quarkus)
  id("org.kordamp.gradle.jandex")
  id("polaris-runtime")
}

dependencies {
  implementation(platform(libs.quarkus.bom))
  implementation("io.quarkus:quarkus-rest-jackson")

  implementation(project(":polaris-extensions-idempotency"))
  implementation(project(":polaris-runtime-service"))

  testImplementation(project(":polaris-core"))
  testImplementation(project(":polaris-relational-jdbc"))
  testImplementation(project(":polaris-runtime-test-common"))

  testImplementation("io.quarkus:quarkus-junit")
  testImplementation("io.quarkus:quarkus-jdbc-h2")
  testImplementation("io.rest-assured:rest-assured")
}

tasks.named("javadoc") { dependsOn("jandex") }

tasks.withType<Test> {
  environment("POLARIS_BOOTSTRAP_CREDENTIALS", "POLARIS,test-admin,test-secret")
  jvmArgs("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED")
  systemProperty("java.security.manager", "allow")
  maxParallelForks = 1
}

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
  // Quarkus platform
  implementation(platform(libs.quarkus.bom))
  implementation("io.quarkus:quarkus-rest-jackson")

  // Add the OPA implementation as RUNTIME dependency to include in Quarkus app
  implementation(project(":polaris-extensions-auth-opa"))

  // Include all runtime-service dependencies
  implementation(project(":polaris-runtime-service"))

  // Test common for integration testing
  testImplementation(project(":polaris-runtime-test-common"))

  // Test dependencies
  intTestImplementation("io.quarkus:quarkus-junit5")
  intTestImplementation("io.rest-assured:rest-assured")

  // Test container dependencies
  intTestImplementation(platform(libs.testcontainers.bom))
  intTestImplementation("org.testcontainers:junit-jupiter")
  intTestImplementation(project(":polaris-container-spec-helper"))
}

tasks.named("javadoc") { dependsOn("jandex") }

tasks.withType<Test> {
  if (System.getenv("AWS_REGION") == null) {
    environment("AWS_REGION", "us-west-2")
  }
  environment("POLARIS_BOOTSTRAP_CREDENTIALS", "POLARIS,test-admin,test-secret")
  jvmArgs("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED")
  systemProperty("java.security.manager", "allow")
  maxParallelForks = 1

  val logsDir = project.layout.buildDirectory.get().asFile.resolve("logs")

  jvmArgumentProviders.add(
    CommandLineArgumentProvider {
      listOf("-Dquarkus.log.file.path=${logsDir.resolve("polaris.log").absolutePath}")
    }
  )

  doFirst {
    logsDir.deleteRecursively()
    project.layout.buildDirectory.get().asFile.resolve("quarkus.log").delete()
  }
}

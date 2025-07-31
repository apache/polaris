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

import org.gradle.api.file.DuplicatesStrategy
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.compile.JavaCompile

plugins {
  id("polaris-java")
  application
}

tasks.withType(JavaCompile::class.java).configureEach { options.release = 21 }

application { mainClass.set("org.apache.polaris.delegation.DelegationServiceApplication") }

dependencies {
  // Core dependencies
  implementation(project(":polaris-core"))
  implementation(project(":polaris-service-common"))
  implementation(project(":polaris-api-delegation-service"))

  // JAX-RS and REST APIs
  implementation(libs.jakarta.ws.rs.api)
  implementation(libs.jakarta.inject.api)
  implementation(libs.jakarta.validation.api)
  implementation(libs.jakarta.enterprise.cdi.api)

  // JSON processing
  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

  // Iceberg for file operations
  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-core")
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-common")

  // Storage backends
  implementation("org.apache.iceberg:iceberg-aws") // S3 support
  implementation("org.apache.iceberg:iceberg-azure") // Azure support
  implementation("org.apache.iceberg:iceberg-gcp") // GCP support

  // Hadoop dependency for FileIO operations
  implementation(libs.hadoop.common)

  // Logging
  implementation(libs.slf4j.api)
  implementation(libs.logback.classic)

  // Testing
  testImplementation(platform(libs.junit.bom))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation(libs.mockito.core)
  testImplementation(libs.assertj.core)
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.register<Jar>("fatJar") {
  archiveBaseName.set("polaris-delegation-service")
  archiveClassifier.set("all")
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
  manifest {
    attributes["Main-Class"] = "org.apache.polaris.delegation.DelegationServiceApplication"
  }
  from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
  with(tasks.jar.get())
}

tasks.build { dependsOn(tasks.named("fatJar")) }

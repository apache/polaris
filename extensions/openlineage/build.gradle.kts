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

import org.gradle.api.attributes.java.TargetJvmVersion
import org.gradle.api.plugins.jvm.JvmTestSuite

plugins {
  id("polaris-server")
  id("org.kordamp.gradle.jandex")
  id("polaris-server-test-runner")
}

val intTestJvmVersion = 21

dependencies {
  // The full Polaris server (with this extension assembled in) that the intTest suite runs against.
  polarisServer(project(path = ":polaris-server", configuration = "quarkusRunner"))

  implementation(project(":polaris-core"))
  // CatalogConfigEndpointContributor and other shared catalog service abstractions.
  implementation(project(":polaris-runtime-service"))
  // The hand-written OpenLineage JAX-RS resource, service interface, and provider seam.
  implementation(project(":polaris-api-openlineage-service"))

  // Endpoint (used by the catalog config endpoint contributor).
  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")

  implementation(libs.jakarta.annotation.api)
  implementation(libs.jakarta.inject.api)
  implementation(libs.jakarta.validation.api)
  implementation(libs.jakarta.ws.rs.api)
  implementation(libs.guava)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")

  compileOnly(libs.microprofile.fault.tolerance.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  testImplementation(platform(libs.junit.bom))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation(libs.assertj.core)
}

testing {
  suites {
    @Suppress("UnstableApiUsage")
    register<JvmTestSuite>("intTest") {
      dependencies {
        implementation(platform(libs.quarkus.bom))
        // Shared integration-test harness: PolarisIntegrationTestExtension, PolarisClient, the
        // OpenLineage client helper, and the PolarisOpenLineageServiceIntegrationTest base class.
        implementation(project(":polaris-tests"))
        implementation(project(":polaris-runtime-test-common"))
        implementation(libs.jakarta.ws.rs.api)
      }
      targets {
        all {
          val buildDir = project.layout.buildDirectory
          testTask.configure {
            environment(
              "AWS_REGION",
              providers.environmentVariable("AWS_REGION").getOrElse("us-west-2"),
            )
            environment(mapOf("POLARIS_BOOTSTRAP_CREDENTIALS" to "POLARIS,test-admin,test-secret"))
            jvmArgs("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED")
            systemProperty("java.security.manager", "allow")
            maxParallelForks = 1

            val buildDirFile = buildDir.get().asFile
            val logsDir = buildDirFile.resolve("logs")

            doFirst {
              logsDir.deleteRecursively()
              buildDirFile.resolve("quarkus.log").delete()
            }

            withPolarisServer(configurations.polarisServer) {
              environment.put(
                "AWS_REGION",
                providers.environmentVariable("AWS_REGION").orElse("us-west-2"),
              )
              environment.putAll(
                mapOf("POLARIS_BOOTSTRAP_CREDENTIALS" to "POLARIS,test-admin,test-secret")
              )
              systemProperties.putAll(
                mapOf(
                  "quarkus.log.file.path" to logsDir.resolve("polaris.log").absolutePath,
                  "polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"" to "[\"FILE\"]",
                  "polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"" to "true",
                  "polaris.readiness.ignore-severe-issues" to "true",
                )
              )
            }
          }
        }
      }
    }
  }
}

listOf("intTestCompileClasspath", "intTestRuntimeClasspath").forEach {
  configurations.named(it) {
    attributes.attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, intTestJvmVersion)
  }
}

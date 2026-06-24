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
  id("polaris-client")
  id("polaris-server-test-runner")
}

val intTestJvmVersion = 21

dependencies { polarisServer(project(path = ":polaris-server", configuration = "quarkusRunner")) }

testing {
  suites {
    @Suppress("UnstableApiUsage")
    register<JvmTestSuite>("intTest") {
      dependencies {
        implementation(platform(libs.quarkus.bom))
        implementation("io.rest-assured:rest-assured")
        implementation(project(":polaris-tests"))
        implementation(project(":polaris-runtime-test-common"))
        implementation(project(":polaris-api-management-model"))
        implementation(platform(libs.iceberg.bom))
        implementation("org.apache.iceberg:iceberg-api")
        implementation("org.apache.iceberg:iceberg-core")

        implementation(platform(libs.testcontainers.bom))
        implementation("org.testcontainers:testcontainers-junit-jupiter")
        implementation(project(":polaris-container-spec-helper"))
      }
      targets {
        val buildDir = project.layout.buildDirectory
        val policyDir = project.layout.projectDirectory.dir("src/intTest/resources/authz_it_tests")
        all {
          testTask.configure {
            environment(
              "AWS_REGION",
              providers.environmentVariable("AWS_REGION").getOrElse("us-west-2"),
            )
            environment(mapOf("POLARIS_BOOTSTRAP_CREDENTIALS" to "POLARIS,test-admin,test-secret"))
            val apiVersion = System.getenv("DOCKER_API_VERSION") ?: "1.44"
            systemProperty("api.version", apiVersion)
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
                  "polaris.authorization.type" to "ranger",
                  "polaris.authorization.ranger.service-name" to "dev_polaris",
                  "polaris.authorization.ranger.authz.default.policy.source.impl" to
                    "org.apache.ranger.admin.client.LocalFolderPolicySource",
                  "polaris.authorization.ranger.authz.default.enable.implicit.userstore.enricher" to
                    "true",
                  "polaris.authorization.ranger.authz.default.policy.source.local_folder.path" to
                    policyDir.asFile.absolutePath,
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
  configurations.named(it).configure {
    attributes.attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, intTestJvmVersion)
  }
}

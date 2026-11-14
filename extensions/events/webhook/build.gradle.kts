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
val webhookStartupAction = sourceSets.create("webhookStartupAction")
val webhookStartupActionCompileOnly = configurations.getByName("webhookStartupActionCompileOnly")

dependencies {
  // The full Polaris server (with this extension assembled in) that the intTest suite runs against.
  polarisServer(project(path = ":polaris-server", configuration = "quarkusRunner"))

  implementation(project(":polaris-core"))
  implementation(project(":polaris-runtime-service"))

  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")

  implementation(platform(libs.jackson3.bom))
  implementation("tools.jackson.core:jackson-databind")

  implementation(platform(libs.quarkus.bom))
  implementation("io.quarkus:quarkus-core")
  implementation("io.quarkus:quarkus-rest-client")
  implementation("io.micrometer:micrometer-core")
  implementation(libs.guava)

  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.smallrye.config.core)

  implementation(libs.slf4j.api)
  implementation(libs.smallrye.common.annotation)

  testImplementation(libs.assertj.core)
  testImplementation(libs.awaitility)
  testImplementation(libs.mockito.junit.jupiter)

  webhookStartupActionCompileOnly(
    "org.apache.polaris.server-test-runner:polaris-server-test-runner",
  )
}

testing {
  suites {
    @Suppress("UnstableApiUsage")
    register<JvmTestSuite>("intTest") {
      dependencies {
        implementation(platform(libs.quarkus.bom))
        implementation(project(":polaris-tests"))
        implementation(project(":polaris-runtime-test-common"))
        implementation(project(":polaris-runtime-service"))
        implementation(project(":polaris-api-management-model"))
        implementation(libs.jakarta.ws.rs.api)
        implementation("org.jboss.resteasy:resteasy-client")
        implementation("org.jboss.resteasy:resteasy-core")
        implementation(platform(libs.jackson.bom))
        implementation("com.fasterxml.jackson.jakarta.rs:jackson-jakarta-rs-json-provider")
        implementation(platform(libs.jackson3.bom))
        implementation("tools.jackson.core:jackson-databind")
        implementation(libs.awaitility)
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
            val captureDirPath = buildDir.dir("webhook-it-captures").get().asFile.absolutePath
            systemProperty("webhook.it.capture-dir", captureDirPath)

            doFirst {
              logsDir.deleteRecursively()
              buildDirFile.resolve("quarkus.log").delete()
            }

            withPolarisServer(configurations.polarisServer) {
              startupActionClasspath.from(webhookStartupAction.runtimeClasspath)
              startupActionClass.set(
                "org.apache.polaris.extensions.events.webhook.test.WebhookReceiverStartupAction",
              )
              startupActionParameters.put("capture-dir", captureDirPath)
              environment.put(
                "AWS_REGION",
                providers.environmentVariable("AWS_REGION").orElse("us-west-2"),
              )
              environment.putAll(
                mapOf("POLARIS_BOOTSTRAP_CREDENTIALS" to "POLARIS,test-admin,test-secret"),
              )
              systemProperties.putAll(
                mapOf(
                  "quarkus.log.file.path" to logsDir.resolve("polaris.log").absolutePath,
                  "polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"" to "[\"FILE\"]",
                  "polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"" to "true",
                  "polaris.readiness.ignore-severe-issues" to "true",
                  "polaris.event-listener.types" to "webhook",
                  "polaris.event-listener.webhook.enabled-event-types" to "AFTER_CREATE_CATALOG",
                  "polaris.event-listener.webhook.require-https" to "false",
                  "polaris.event-listener.webhook.secret" to "webhook-it-secret",
                  "polaris.event-listener.webhook.headers.\"Authorization\"" to
                    "Bearer webhook-it-token",
                  "polaris.event-listener.webhook.headers.\"Polaris-Signature-256\"" to
                    "sha256=forged",
                ),
              )
            }
          }
        }
      }
    }
  }
}

listOf(
    "intTestCompileClasspath",
    "intTestRuntimeClasspath",
    "webhookStartupActionCompileClasspath",
    "webhookStartupActionRuntimeClasspath",
  )
  .forEach {
    configurations.named(it) {
      attributes.attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, intTestJvmVersion)
    }
  }

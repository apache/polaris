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
import org.gradle.language.base.plugins.LifecycleBasePlugin

plugins {
  id("polaris-client")
  id("polaris-server-test-runner")
}

val intTestJvmVersion = 21
val intTestBase by configurations.creating {
  isCanBeConsumed = false
  isCanBeResolved = false
}
val intTestBaseSources = sourceSets.create("intTestBase")
val opaStartupAction = sourceSets.create("opaStartupAction")
val opaStartupActionCompileOnly by configurations.getting
val opaStartupActionImplementation by configurations.getting
val opaBearerTokenRefreshIntervalProperty =
  "polaris.authorization.opa.auth.bearer.file-based.refresh-interval"

listOf(
    intTestBaseSources.implementationConfigurationName,
    opaStartupAction.runtimeOnlyConfigurationName,
  )
  .forEach { configurations.named(it) { extendsFrom(intTestBase) } }

dependencies {
  polarisServer(project(path = ":polaris-server", configuration = "quarkusRunner"))

  opaStartupActionCompileOnly("org.apache.polaris.server-test-runner:polaris-server-test-runner")
  opaStartupActionImplementation(platform(libs.testcontainers.bom))
  opaStartupActionImplementation("org.testcontainers:testcontainers")
  opaStartupActionImplementation(project(":polaris-container-spec-helper"))

  intTestBase(platform(libs.junit.bom))
  intTestBase("org.junit.jupiter:junit-jupiter")
  intTestBase("org.junit.platform:junit-platform-launcher")
  intTestBase(enforcedPlatform(libs.quarkus.bom))
  intTestBase("io.rest-assured:rest-assured")
  intTestBase(project(":polaris-tests"))
  intTestBase(project(":polaris-runtime-test-common"))
  intTestBase(project(":polaris-api-management-model"))
  intTestBase(platform(libs.iceberg.bom))
  intTestBase("org.apache.iceberg:iceberg-api")
  intTestBase("org.apache.iceberg:iceberg-core")
}

fun Test.configureOpaTestTask(
  buildDir: DirectoryProperty,
  staticToken: String? = null,
  fileTokenPath: Provider<String>? = null,
) {
  description = "Runs OPA integration tests against an external Polaris server."
  group = LifecycleBasePlugin.VERIFICATION_GROUP

  val apiVersion = providers.environmentVariable("DOCKER_API_VERSION").getOrElse("1.44")
  systemProperty("api.version", apiVersion)
  jvmArgs("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED")
  systemProperty("java.security.manager", "allow")

  val logsDir = buildDir.get().asFile.resolve("logs/$name")

  doFirst {
    logsDir.deleteRecursively()
    buildDir.get().asFile.resolve("quarkus.log").delete()
  }

  withPolarisServer(configurations.polarisServer) {
    startupActionClasspath.from(opaStartupAction.runtimeClasspath)
    startupActionClass.set("org.apache.polaris.extension.auth.opa.test.OpaStartupAction")

    environment.put("AWS_REGION", providers.environmentVariable("AWS_REGION").orElse("us-west-2"))
    environment.putAll(mapOf("POLARIS_BOOTSTRAP_CREDENTIALS" to "POLARIS,test-admin,test-secret"))
    systemProperties.putAll(
      mapOf(
        "quarkus.log.file.path" to logsDir.resolve("polaris.log").absolutePath,
        "polaris.authorization.type" to "opa",
        "polaris.authorization.opa.auth.type" to "bearer",
        "polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"" to "[\"FILE\"]",
        "polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"" to "true",
        "polaris.readiness.ignore-severe-issues" to "true",
      )
    )
    staticToken?.let {
      systemProperties.putAll(
        mapOf("polaris.authorization.opa.auth.bearer.static-token.value" to it)
      )
    }
    fileTokenPath?.orNull?.let {
      systemProperties.putAll(
        mapOf(
          "polaris.authorization.opa.auth.bearer.file-based.path" to it,
          opaBearerTokenRefreshIntervalProperty to "PT1S",
        )
      )
    }
  }
}

@Suppress("UnstableApiUsage")
fun JvmTestSuite.configureIntTest() {
  useJUnitJupiter()

  configurations.named(sources.implementationConfigurationName) { extendsFrom(intTestBase) }
  sources.compileClasspath += intTestBaseSources.output
  sources.runtimeClasspath += intTestBaseSources.output
}

testing {
  suites {
    val buildDir = project.layout.buildDirectory

    @Suppress("UnstableApiUsage")
    register<JvmTestSuite>("bearerTokenIntTest") {
      configureIntTest()
      targets {
        all {
          testTask.configure {
            configureOpaTestTask(buildDir, staticToken = "test-opa-bearer-token-12345")
          }
        }
      }
    }

    @Suppress("UnstableApiUsage")
    register<JvmTestSuite>("opaFileTokenIntTest") {
      configureIntTest()
      val tokenFile = buildDir.file("opa-file-token/token.txt")
      targets {
        all {
          testTask.configure {
            configureOpaTestTask(buildDir, fileTokenPath = tokenFile.map { it.asFile.absolutePath })

            doFirst {
              val file = tokenFile.get().asFile
              file.parentFile.mkdirs()
              file.writeText("test-opa-bearer-token-from-file")
            }
          }
        }
      }
    }
  }
}

listOf(
    intTestBaseSources.compileClasspathConfigurationName,
    intTestBaseSources.runtimeClasspathConfigurationName,
    "bearerTokenIntTestCompileClasspath",
    "bearerTokenIntTestRuntimeClasspath",
    "opaFileTokenIntTestCompileClasspath",
    "opaFileTokenIntTestRuntimeClasspath",
    "opaStartupActionCompileClasspath",
    "opaStartupActionRuntimeClasspath",
  )
  .forEach {
    configurations.named(it).configure {
      attributes.attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, intTestJvmVersion)
    }
  }

tasks.register("intTest") {
  description = "Runs all OPA integration tests."
  group = LifecycleBasePlugin.VERIFICATION_GROUP
  dependsOn("bearerTokenIntTest", "opaFileTokenIntTest")
}

tasks.named("check") { dependsOn("intTest") }

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

val polarisRangerServiceDefFile =
  layout.projectDirectory.file("src/main/resources/polaris-ranger-servicedef.json")
val servicedefPlaceholder = "@@POLARIS_RANGER_SERVICE_DEF@@"

// Strips the ASF license header (required since these templates are checked-in source files)
// that precedes the "{" starting the actual, not-quite-valid-JSON template content.
fun stripLicenseHeader(text: String) = text.substringAfter("*/").trimStart()

// The intTest authz fixture asserts against the shipped `serviceDef`, so rather than checking
// in a byte-for-byte copy of it (which can silently drift), it's a template with the shipped
// `polaris-ranger-servicedef.json` spliced in at build time. The unit-test equivalent fixture is
// generated the same way, but by test Java code at test-run time (see RangerTestUtils), since
// RangerPolarisAuthorizerTest runs in-process and can just write into a JUnit-managed temp dir.
val generateAuthzItTestFixture =
  tasks.register("generateAuthzItTestFixture") {
    val templateFile =
      layout.projectDirectory.file("src/intTest/resources/authz_it_tests/dev_polaris.json.template")
    val rolesFile =
      layout.projectDirectory.file("src/intTest/resources/authz_it_tests/dev_polaris_roles.json")
    val userStoreFile =
      layout.projectDirectory.file(
        "src/intTest/resources/authz_it_tests/dev_polaris_userstore.json"
      )
    val outputDir = layout.buildDirectory.dir("generated/resources/intTest/authz_it_tests")
    inputs.file(polarisRangerServiceDefFile)
    inputs.file(templateFile)
    inputs.file(rolesFile)
    inputs.file(userStoreFile)
    outputs.dir(outputDir)
    doLast {
      // The sensitivity-based IT authz test exercises a boolean-expression policy condition
      // that isn't part of the operator-facing artifact, so it's appended to the shipped
      // serviceDef here rather than shipped in polaris-ranger-servicedef.json itself.
      val policyConditions =
        """
        ,
        "policyConditions": [
          {
            "itemId": 1,
            "name": "_expression",
            "evaluator": "org.apache.ranger.plugin.conditionevaluator.RangerScriptConditionEvaluator",
            "evaluatorOptions": { "engineName": "JavaScript" },
            "label": "Enter boolean expression",
            "description": "Boolean expression"
          }
        ]
        """
          .trimIndent()
      val shippedServiceDef = polarisRangerServiceDefFile.asFile.readText().trim()
      check(shippedServiceDef.endsWith("}")) {
        "unexpected trailing content in $polarisRangerServiceDefFile"
      }
      val serviceDefWithConditions =
        shippedServiceDef.removeSuffix("}").trimEnd() + policyConditions + "\n}"

      val template = stripLicenseHeader(templateFile.asFile.readText())
      val merged = template.replace(servicedefPlaceholder, serviceDefWithConditions)
      val outDir = outputDir.get().asFile.apply { mkdirs() }
      outDir.resolve("dev_polaris.json").writeText(merged)
      rolesFile.asFile.copyTo(outDir.resolve("dev_polaris_roles.json"), overwrite = true)
      userStoreFile.asFile.copyTo(outDir.resolve("dev_polaris_userstore.json"), overwrite = true)
    }
  }

dependencies {
  polarisServer(project(path = ":polaris-server", configuration = "quarkusRunner"))

  implementation(project(":polaris-core"))

  implementation(libs.ranger.authz.embedded) {
    exclude("org.apache.ranger", "ranger-audit-dest-hdfs")
    exclude("org.slf4j", "slf4j-reload4j")
    exclude("ch.qos.reload4j", "reload4j")
    exclude("io.dropwizard.metrics", "metrics-core")
  }

  implementation(libs.commons.lang3)
  implementation(libs.guava)

  // Iceberg dependency for ForbiddenException
  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")

  testImplementation(platform(libs.jackson3.bom))
  testImplementation("tools.jackson.core:jackson-databind")

  compileOnly(libs.jspecify)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.smallrye.config.core)
  compileOnly(project(":polaris-immutables"))

  runtimeOnly(libs.graalvm.js.js.scriptengine)
  runtimeOnly(libs.graalvm.polyglot.js)
  runtimeOnly(libs.graalvm.polyglot.polyglot)
}

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
        implementation(platform(libs.jackson3.bom))
        implementation("tools.jackson.core:jackson-databind")

        implementation(platform(libs.testcontainers.bom))
        implementation("org.testcontainers:testcontainers-junit-jupiter")
        implementation(project(":polaris-container-spec-helper"))
      }
      targets {
        all {
          val buildDir = project.layout.buildDirectory
          val policyDir = buildDir.dir("generated/resources/intTest/authz_it_tests")
          testTask.configure {
            dependsOn(generateAuthzItTestFixture)
            environment(
              "AWS_REGION",
              providers.environmentVariable("AWS_REGION").getOrElse("us-west-2"),
            )
            environment(mapOf("POLARIS_BOOTSTRAP_CREDENTIALS" to "POLARIS,test-admin,test-secret"))
            val apiVersion = providers.environmentVariable("DOCKER_API_VERSION").getOrElse("1.44")
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
                    policyDir.get().asFile.absolutePath,
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

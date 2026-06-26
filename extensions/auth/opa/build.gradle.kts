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

import java.io.OutputStream
import org.gradle.api.attributes.java.TargetJvmVersion
import org.gradle.api.plugins.jvm.JvmTestSuite
import org.gradle.language.base.plugins.LifecycleBasePlugin

plugins {
  id("polaris-server")
  id("polaris-server-test-runner")
  id("org.kordamp.gradle.jandex")
}

val intTestJvmVersion = 21
val intTestBase by configurations.creating {
  isCanBeConsumed = false
  isCanBeResolved = false
}
val intTestBaseSources = sourceSets.create("intTestBase")
val jsonSchemaGenerator = sourceSets.create("jsonSchemaGenerator")
val jsonSchemaGeneratorImplementation by configurations.getting
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

  implementation(project(":polaris-core"))
  implementation(libs.apache.httpclient5)
  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation(libs.guava)
  implementation(libs.slf4j.api)
  implementation(libs.auth0.jwt)
  implementation(project(":polaris-async-api"))

  jsonSchemaGeneratorImplementation(project(":polaris-extensions-auth-opa"))
  jsonSchemaGeneratorImplementation(platform(libs.jackson.bom))
  jsonSchemaGeneratorImplementation("com.fasterxml.jackson.module:jackson-module-jsonSchema")

  // Iceberg dependency for ForbiddenException
  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  compileOnly(libs.jspecify)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.smallrye.config.core)

  testCompileOnly(project(":polaris-immutables"))
  testAnnotationProcessor(project(":polaris-immutables", configuration = "processor"))

  testImplementation(testFixtures(project(":polaris-core")))
  testImplementation(platform(libs.junit.bom))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation(libs.assertj.core)
  testImplementation(libs.mockito.core)
  testImplementation(libs.threeten.extra)
  testImplementation(testFixtures(project(":polaris-async-api")))
  testImplementation(project(":polaris-async-java"))
  testImplementation(project(":polaris-idgen-mocks"))

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

// Task to generate JSON Schema from model classes
tasks.register<JavaExec>("generateOpaSchema") {
  group = "documentation"
  description = "Generates JSON Schema for OPA authorization input"

  dependsOn(tasks.compileJava, tasks.named("jandex"))

  // Only execute generation if anything changed
  outputs.cacheIf { true }
  outputs.file("./opa-input-schema.json")
  inputs.files(jsonSchemaGenerator.runtimeClasspath).withNormalizer(ClasspathNormalizer::class.java)

  classpath = jsonSchemaGenerator.runtimeClasspath
  mainClass.set("org.apache.polaris.extension.auth.opa.model.OpaSchemaGenerator")
  args("./opa-input-schema.json")
}

// Task to validate that the committed schema matches the generated schema
tasks.register<JavaExec>("validateOpaSchema") {
  group = "verification"
  description = "Validates that the committed OPA schema matches the generated schema"

  dependsOn(tasks.compileJava, tasks.named("jandex"))

  val tempSchemaFile =
    layout.buildDirectory
      .file("opa-schema/opa-input-schema-generated.json")
      .get()
      .asFile
      .relativeTo(projectDir)
  val committedSchemaFile = file("${projectDir}/opa-input-schema.json")
  val logFile = layout.buildDirectory.file("opa-schema/generator.log")

  // Only execute validation if anything changed
  outputs.cacheIf { true }
  outputs.file(tempSchemaFile)
  inputs.file(committedSchemaFile).withPathSensitivity(PathSensitivity.RELATIVE)
  inputs.files(jsonSchemaGenerator.runtimeClasspath).withNormalizer(ClasspathNormalizer::class.java)

  classpath = jsonSchemaGenerator.runtimeClasspath
  mainClass.set("org.apache.polaris.extension.auth.opa.model.OpaSchemaGenerator")
  args(tempSchemaFile)
  isIgnoreExitValue = true

  var outStream: OutputStream? = null
  doFirst {
    // Ensure temp directory exists
    tempSchemaFile.parentFile.mkdirs()
    outStream = logFile.get().asFile.outputStream()
    standardOutput = outStream
    errorOutput = outStream
  }

  val prjDir = projectDir

  doLast {
    outStream?.close()

    if (executionResult.get().exitValue != 0) {
      throw GradleException(
        """
        |OPA Schema validation failed!
        |
        |${logFile.get().asFile.readText()}
      """
          .trimMargin()
      )
    }

    val generatedContent = prjDir.resolve(tempSchemaFile).readText().trim()
    val committedContent = committedSchemaFile.readText().trim()

    if (generatedContent != committedContent) {
      throw GradleException(
        """
        |OPA Schema validation failed!
        |
        |The committed opa-input-schema.json does not match the generated schema.
        |This means the schema is out of sync with the model classes.
        |
        |To fix this, run:
        |  ./gradlew :polaris-extensions-auth-opa:generateOpaSchema
        |
        |Then commit the updated opa-input-schema.json file.
        |
        |Committed file: ${committedSchemaFile.absolutePath}
        |Generated file: ${tempSchemaFile.absolutePath}
      """
          .trimMargin()
      )
    }

    logger.info("OPA schema validation passed - schema is up to date")
  }
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

tasks.named("check") { dependsOn("validateOpaSchema", "intTest") }

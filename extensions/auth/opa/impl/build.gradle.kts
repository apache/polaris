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

plugins {
  id("polaris-server")
  id("org.kordamp.gradle.jandex")
}

val jsonSchemaGenerator = sourceSets.create("jsonSchemaGenerator")

dependencies {
  implementation(project(":polaris-core"))
  implementation(libs.apache.httpclient5)
  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.module:jackson-module-jsonSchema")
  implementation(libs.guava)
  implementation(libs.slf4j.api)
  implementation(libs.auth0.jwt)
  implementation(project(":polaris-async-api"))

  add(jsonSchemaGenerator.implementationConfigurationName, project(":polaris-extensions-auth-opa"))
  add(jsonSchemaGenerator.implementationConfigurationName, platform(libs.jackson.bom))
  add(
    jsonSchemaGenerator.implementationConfigurationName,
    "com.fasterxml.jackson.module:jackson-module-jsonSchema",
  )

  // Iceberg dependency for ForbiddenException
  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

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
}

// Task to generate JSON Schema from model classes
tasks.register<JavaExec>("generateOpaSchema") {
  group = "documentation"
  description = "Generates JSON Schema for OPA authorization input"

  dependsOn(tasks.compileJava, tasks.named("jandex"))

  // Only execute generation if anything changed
  outputs.cacheIf { true }
  outputs.file("${projectDir}/opa-input-schema.json")
  inputs.files(jsonSchemaGenerator.runtimeClasspath)

  classpath = jsonSchemaGenerator.runtimeClasspath
  mainClass.set("org.apache.polaris.extension.auth.opa.model.OpaSchemaGenerator")
  args("${projectDir}/opa-input-schema.json")
}

// Task to validate that the committed schema matches the generated schema
tasks.register<JavaExec>("validateOpaSchema") {
  group = "verification"
  description = "Validates that the committed OPA schema matches the generated schema"

  dependsOn(tasks.compileJava, tasks.named("jandex"))

  val tempSchemaFile = layout.buildDirectory.file("opa-schema/opa-input-schema-generated.json")
  val committedSchemaFile = file("${projectDir}/opa-input-schema.json")
  val logFile = layout.buildDirectory.file("opa-schema/generator.log")

  // Only execute validation if anything changed
  outputs.cacheIf { true }
  outputs.file(tempSchemaFile)
  inputs.file(committedSchemaFile)
  inputs.files(jsonSchemaGenerator.runtimeClasspath)

  classpath = jsonSchemaGenerator.runtimeClasspath
  mainClass.set("org.apache.polaris.extension.auth.opa.model.OpaSchemaGenerator")
  args(tempSchemaFile.get().asFile.absolutePath)
  isIgnoreExitValue = true

  var outStream: OutputStream? = null
  doFirst {
    // Ensure temp directory exists
    tempSchemaFile.get().asFile.parentFile.mkdirs()
    outStream = logFile.get().asFile.outputStream()
    standardOutput = outStream
    errorOutput = outStream
  }

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

    val generatedContent = tempSchemaFile.get().asFile.readText().trim()
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
        |Generated file: ${tempSchemaFile.get().asFile.absolutePath}
      """
          .trimMargin()
      )
    }

    logger.info("OPA schema validation passed - schema is up to date")
  }
}

// Add schema validation to the check task
tasks.named("check") { dependsOn("validateOpaSchema") }

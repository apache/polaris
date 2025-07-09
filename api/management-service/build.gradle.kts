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

import org.openapitools.generator.gradle.plugin.tasks.GenerateTask

plugins {
  alias(libs.plugins.openapi.generator)
  id("polaris-client")
  alias(libs.plugins.jandex)
}

dependencies {
  implementation(project(":polaris-api-management-model"))
  implementation(project(":polaris-core"))

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.microprofile.fault.tolerance.api)
  compileOnly(libs.swagger.annotations)

  implementation(libs.jakarta.servlet.api)
  implementation(libs.jakarta.ws.rs.api)

  compileOnly(platform(libs.micrometer.bom))
  compileOnly("io.micrometer:micrometer-core")

  implementation(libs.slf4j.api)
}

val rootDir = rootProject.layout.projectDirectory
val specsDir = rootDir.dir("spec")
val templatesDir = rootDir.dir("server-templates")
// Use a different directory than 'generated/', because OpenAPI generator's `GenerateTask` adds the
// whole directory to its task output, but 'generated/' is not exclusive to that task and in turn
// breaks Gradle's caching.
val generatedDir = project.layout.buildDirectory.dir("generated-openapi")
val generatedOpenApiSrcDir = project.layout.buildDirectory.dir("generated-openapi/src/main/java")

openApiGenerate {
  // The OpenAPI generator does NOT resolve relative paths correctly against the Gradle project
  // directory
  inputSpec = specsDir.file("polaris-management-service.yml").asFile.absolutePath
  generatorName = "jaxrs-resteasy"
  outputDir = generatedDir.get().asFile.absolutePath
  apiPackage = "org.apache.polaris.service.admin.api"
  modelPackage = "org.apache.polaris.core.admin.model"
  ignoreFileOverride = rootDir.file(".openapi-generator-ignore").asFile.absolutePath
  removeOperationIdPrefix = true
  templateDir = templatesDir.asFile.absolutePath
  globalProperties.put("apis", "")
  globalProperties.put("models", "false")
  globalProperties.put("apiDocs", "false")
  globalProperties.put("modelTests", "false")
  configOptions.put("useBeanValidation", "true")
  configOptions.put("sourceFolder", "src/main/java")
  configOptions.put("useJakartaEe", "true")
  configOptions.put("generateBuilders", "true")
  configOptions.put("generateConstructorWithAllArgs", "true")
  configOptions.put("hideGenerationTimestamp", "true")
  additionalProperties.put("apiNamePrefix", "Polaris")
  additionalProperties.put("apiNameSuffix", "Api")
  additionalProperties.put("metricsPrefix", "polaris")
  serverVariables.put("basePath", "api/v1")
}

listOf("sourcesJar", "compileJava", "processResources").forEach { task ->
  tasks.named(task) { dependsOn("openApiGenerate") }
}

sourceSets { main { java { srcDir(generatedOpenApiSrcDir) } } }

tasks.named<GenerateTask>("openApiGenerate") {
  inputs.dir(templatesDir)
  inputs.dir(specsDir)
  actions.addFirst { delete { delete(generatedDir) } }
}

tasks.named("javadoc") { dependsOn("jandex") }

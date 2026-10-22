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
  id("org.kordamp.gradle.jandex")
}

val metricsModels =
  listOf(
      "QueryMetricsRequest",
      "ListMetricsResponse",
      "MetricsReport",
      "MetricsRequest",
      "TableRef",
    )
    .joinToString(",")

dependencies {
  implementation(project(":polaris-core"))

  // The generated API uses Iceberg's ErrorResponse and TableIdentifier-related types (mirrors the
  // set used by the polaris-api-catalog-service generated code).
  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")

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

val rootDir = layout.settingsDirectory
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
  inputSpec = specsDir.file("generated/bundled-polaris-catalog-service.yaml")
  generatorName = "jaxrs-resteasy"
  outputDir = generatedDir
  apiPackage = "org.apache.polaris.service.metrics.api"
  modelPackage = "org.apache.polaris.core.metrics.api.model"
  ignoreFileOverride = rootDir.file(".openapi-generator-ignore")
  removeOperationIdPrefix.set(true)
  templateDir = templatesDir
  globalProperties.put("apis", "MetricsReportsApi")
  globalProperties.put("models", metricsModels)
  globalProperties.put("apiDocs", "false")
  globalProperties.put("modelTests", "false")
  configOptions.put("openApiNullable", "false")
  configOptions.put("useBeanValidation", "true")
  configOptions.put("resourceName", "catalog")
  configOptions.put("useTags", "true")
  configOptions.put("sourceFolder", "src/main/java")
  configOptions.put("useJakartaEe", "true")
  configOptions.put("generateBuilders", "true")
  configOptions.put("generateConstructorWithAllArgs", "true")
  configOptions.put("hideGenerationTimestamp", "true")
  additionalProperties.put("apiNamePrefix", "PolarisCatalog")
  additionalProperties.put("apiNameSuffix", "")
  additionalProperties.put("metricsPrefix", "polaris")
  serverVariables.put("basePath", "api/catalog")
  importMappings =
    mapOf(
      "ErrorModel" to "org.apache.iceberg.rest.responses.ErrorResponse",
      "IcebergErrorResponse" to "org.apache.iceberg.rest.responses.ErrorResponse",
    )
}

listOf("sourcesJar", "compileJava", "processResources").forEach { task ->
  tasks.named(task) { dependsOn("openApiGenerate") }
}

sourceSets { main { java { srcDir(generatedOpenApiSrcDir) } } }

tasks.named<GenerateTask>("openApiGenerate") {
  inputs.dir(templatesDir)
  inputs.dir(specsDir)
  cleanupOutput = true
}

tasks.named("javadoc") { dependsOn("jandex") }

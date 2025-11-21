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

dependencies {
  implementation(project(":polaris-core"))

  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")
  implementation("org.apache.iceberg:iceberg-aws")

  implementation(libs.jakarta.annotation.api)
  implementation(libs.jakarta.inject.api)
  implementation(libs.jakarta.validation.api)
  implementation(libs.swagger.annotations)

  implementation(libs.jakarta.servlet.api)
  implementation(libs.jakarta.ws.rs.api)

  implementation(platform(libs.micrometer.bom))
  implementation("io.micrometer:micrometer-core")

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")

  compileOnly(libs.microprofile.fault.tolerance.api)

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))
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
  inputSpec = provider { specsDir.file("s3-sign/polaris-s3-sign-service.yaml").asFile.absolutePath }
  generatorName = "jaxrs-resteasy"
  outputDir = provider { generatedDir.get().asFile.absolutePath }
  apiPackage = "org.apache.polaris.service.s3.sign.api"
  ignoreFileOverride.set(provider { rootDir.file(".openapi-generator-ignore").asFile.absolutePath })
  templateDir.set(provider { templatesDir.asFile.absolutePath })
  removeOperationIdPrefix.set(true)
  globalProperties.put("apis", "S3SignerApi")
  globalProperties.put("models", "false")
  globalProperties.put("apiDocs", "false")
  globalProperties.put("modelTests", "false")
  configOptions.put("resourceName", "catalog")
  configOptions.put("useTags", "true")
  configOptions.put("useBeanValidation", "false")
  configOptions.put("sourceFolder", "src/main/java")
  configOptions.put("useJakartaEe", "true")
  configOptions.put("hideGenerationTimestamp", "true")
  additionalProperties.put("apiNamePrefix", "IcebergRest")
  additionalProperties.put("apiNameSuffix", "")
  additionalProperties.put("metricsPrefix", "polaris")
  serverVariables.put("basePath", "api/s3-sign")
  modelNameMappings = mapOf("S3SignRequest" to "PolarisS3SignRequest")
  typeMappings =
    mapOf("S3SignRequest" to "org.apache.polaris.service.s3.sign.model.PolarisS3SignRequest")
  importMappings =
    mapOf(
      "IcebergErrorResponse" to "org.apache.iceberg.rest.responses.ErrorResponse",
      "PolarisS3SignRequest" to "org.apache.polaris.service.s3.sign.model.PolarisS3SignRequest",
      "SignS3Request200Response" to "org.apache.polaris.service.s3.sign.model.PolarisS3SignResponse",
    )
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

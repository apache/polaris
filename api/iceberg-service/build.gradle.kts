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
  implementation(project(":polaris-core"))

  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")

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
  inputSpec = specsDir.file("polaris-catalog-service.yaml").asFile.absolutePath
  generatorName = "jaxrs-resteasy"
  outputDir = generatedDir.get().asFile.absolutePath
  apiPackage = "org.apache.polaris.service.catalog.api"
  ignoreFileOverride = rootDir.file(".openapi-generator-ignore").asFile.absolutePath
  removeOperationIdPrefix = true
  templateDir = templatesDir.asFile.absolutePath
  globalProperties.put("apis", "CatalogApi,ConfigurationApi,OAuth2Api")
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
  serverVariables.put("basePath", "api/catalog")
  importMappings =
    mapOf(
      "CatalogConfig" to "org.apache.iceberg.rest.responses.ConfigResponse",
      "CommitTableResponse" to "org.apache.iceberg.rest.responses.LoadTableResponse",
      "CreateNamespaceRequest" to "org.apache.iceberg.rest.requests.CreateNamespaceRequest",
      "CreateNamespaceResponse" to "org.apache.iceberg.rest.responses.CreateNamespaceResponse",
      "CreateTableRequest" to "org.apache.iceberg.rest.requests.CreateTableRequest",
      "ErrorModel" to "org.apache.iceberg.rest.responses.ErrorResponse",
      "GetNamespaceResponse" to "org.apache.iceberg.rest.responses.GetNamespaceResponse",
      "ListNamespacesResponse" to "org.apache.iceberg.rest.responses.ListNamespacesResponse",
      "ListTablesResponse" to "org.apache.iceberg.rest.responses.ListTablesResponse",
      "LoadCredentialsResponse" to "org.apache.iceberg.rest.responses.LoadCredentialsResponse",
      "LoadTableResult" to "org.apache.iceberg.rest.responses.LoadTableResponse",
      "LoadViewResult" to "org.apache.iceberg.rest.responses.LoadTableResponse",
      "OAuthTokenResponse" to "org.apache.iceberg.rest.responses.OAuthTokenResponse",
      "OAuthErrorResponse" to "org.apache.iceberg.rest.responses.OAuthErrorResponse",
      "RenameTableRequest" to "org.apache.iceberg.rest.requests.RenameTableRequest",
      "ReportMetricsRequest" to "org.apache.iceberg.rest.requests.ReportMetricsRequest",
      "UpdateNamespacePropertiesRequest" to
        "org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest",
      "UpdateNamespacePropertiesResponse" to
        "org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse",
      "CommitTransactionRequest" to "org.apache.iceberg.rest.requests.CommitTransactionRequest",
      "CreateViewRequest" to "org.apache.iceberg.rest.requests.CreateViewRequest",
      "RegisterTableRequest" to "org.apache.iceberg.rest.requests.RegisterTableRequest",
      "IcebergErrorResponse" to "org.apache.iceberg.rest.responses.ErrorResponse",
      "OAuthError" to "org.apache.iceberg.rest.responses.ErrorResponse",

      // Custom types defined below
      "CommitViewRequest" to "org.apache.polaris.service.types.CommitViewRequest",
      "TokenType" to "org.apache.polaris.service.types.TokenType",
      "CommitTableRequest" to "org.apache.polaris.service.types.CommitTableRequest",
      "NotificationRequest" to "org.apache.polaris.service.types.NotificationRequest",
      "TableUpdateNotification" to "org.apache.polaris.service.types.TableUpdateNotification",
      "NotificationType" to "org.apache.polaris.service.types.NotificationType",
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

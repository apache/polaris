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
  id("polaris-server")
}

dependencies {
  implementation(project(":polaris-core"))

  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")
  implementation("org.apache.iceberg:iceberg-aws")

  implementation(libs.hadoop.common) {
    exclude("org.slf4j", "slf4j-reload4j")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("ch.qos.reload4j", "reload4j")
    exclude("log4j", "log4j")
    exclude("org.apache.zookeeper", "zookeeper")
    exclude("org.apache.hadoop.thirdparty", "hadoop-shaded-protobuf_3_25")
    exclude("com.github.pjfanning", "jersey-json")
    exclude("com.sun.jersey", "jersey-core")
    exclude("com.sun.jersey", "jersey-server")
    exclude("com.sun.jersey", "jersey-servlet")
  }
  implementation(libs.hadoop.hdfs.client)

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.servlet.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.ws.rs.api)

  compileOnly(libs.smallrye.common.annotation)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-xml")
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  implementation(libs.caffeine)
  implementation(libs.guava)
  implementation(libs.slf4j.api)

  compileOnly(libs.swagger.annotations)
  compileOnly(libs.spotbugs.annotations)
  implementation(libs.swagger.jaxrs)

  implementation(libs.hadoop.client.api)

  implementation(libs.auth0.jwt)

  implementation(libs.logback.core)
  implementation(libs.bouncycastle.bcprov)

  implementation(platform(libs.google.cloud.storage.bom))
  implementation("com.google.cloud:google-cloud-storage")

  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:sts")
  implementation("software.amazon.awssdk:iam-policy-builder")
  implementation("software.amazon.awssdk:s3")

  implementation(platform(libs.azuresdk.bom))
  implementation("com.azure:azure-core")

  implementation(platform(libs.micrometer.bom))
  compileOnly("io.micrometer:micrometer-core")
}

openApiGenerate {
  inputSpec = "$rootDir/spec/rest-catalog-open-api.yaml"
  generatorName = "jaxrs-resteasy"
  outputDir = "$projectDir/build/generated"
  apiPackage = "org.apache.polaris.service.catalog.api"
  ignoreFileOverride = "$rootDir/.openapi-generator-ignore"
  removeOperationIdPrefix = true
  templateDir = "$rootDir/server-templates"
  globalProperties.put("apis", "")
  globalProperties.put("models", "false")
  globalProperties.put("apiDocs", "false")
  globalProperties.put("modelTests", "false")
  configOptions.put("resourceName", "catalog")
  configOptions.put("useTags", "true")
  configOptions.put("useBeanValidation", "false")
  configOptions.put("sourceFolder", "src/main/java")
  configOptions.put("useJakartaEe", "true")
  openapiNormalizer.put("REFACTOR_ALLOF_WITH_PROPERTIES_ONLY", "true")
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
      "NotificationType" to "org.apache.polaris.service.types.NotificationType"
    )
}

val generatePolarisService by
  tasks.registering(GenerateTask::class) {
    inputSpec = "$rootDir/spec/polaris-management-service.yml"
    generatorName = "jaxrs-resteasy"
    outputDir = "$projectDir/build/generated"
    apiPackage = "org.apache.polaris.service.admin.api"
    modelPackage = "org.apache.polaris.core.admin.model"
    ignoreFileOverride = "$rootDir/.openapi-generator-ignore"
    removeOperationIdPrefix = true
    templateDir = "$rootDir/server-templates"
    globalProperties.put("apis", "")
    globalProperties.put("models", "false")
    globalProperties.put("apiDocs", "false")
    globalProperties.put("modelTests", "false")
    configOptions.put("useBeanValidation", "true")
    configOptions.put("sourceFolder", "src/main/java")
    configOptions.put("useJakartaEe", "true")
    configOptions.put("generateBuilders", "true")
    configOptions.put("generateConstructorWithAllArgs", "true")
    additionalProperties.put("apiNamePrefix", "Polaris")
    additionalProperties.put("apiNameSuffix", "Api")
    additionalProperties.put("metricsPrefix", "polaris")
    serverVariables.put("basePath", "api/v1")
  }

listOf("sourcesJar", "compileJava").forEach { task ->
  tasks.named(task) { dependsOn("openApiGenerate", generatePolarisService) }
}

sourceSets {
  main { java { srcDir(project.layout.buildDirectory.dir("generated/src/main/java")) } }
}

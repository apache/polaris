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

plugins {
  alias(libs.plugins.openapi.generator)
  id("polaris-client")
  alias(libs.plugins.jandex)
}

val genericTableModels =
  listOf(
    "CreateGenericTableRequest",
    "LoadGenericTableResponse",
    "ListGenericTablesResponse",
    "GenericTable",
  )

val policyManagementModels =
  listOf(
    "CatalogIdentifier",
    "CreatePolicyRequest",
    "LoadPolicyResponse",
    "PolicyIdentifier",
    "Policy",
    "PolicyAttachmentTarget",
    "AttachPolicyRequest",
    "DetachPolicyRequest",
    "UpdatePolicyRequest",
    "GetApplicablePoliciesResponse",
    "ListPoliciesResponse",
  )

val models = (genericTableModels + policyManagementModels).joinToString(",")

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
}

openApiGenerate {
  inputSpec = "$rootDir/spec/polaris-catalog-service.yaml"
  generatorName = "jaxrs-resteasy"
  outputDir = "$projectDir/build/generated"
  apiPackage = "org.apache.polaris.service.catalog.api"
  modelPackage = "org.apache.polaris.service.types"
  ignoreFileOverride = "$rootDir/.openapi-generator-ignore"
  removeOperationIdPrefix = true
  templateDir = "$rootDir/server-templates"
  globalProperties.put("apis", "GenericTableApi,PolicyApi")
  globalProperties.put("models", models)
  globalProperties.put("apiDocs", "false")
  globalProperties.put("modelTests", "false")
  configOptions.put("resourceName", "catalog")
  configOptions.put("useTags", "true")
  configOptions.put("useBeanValidation", "false")
  configOptions.put("sourceFolder", "src/main/java")
  configOptions.put("useJakartaEe", "true")
  configOptions.put("generateBuilders", "true")
  configOptions.put("generateConstructorWithAllArgs", "true")
  configOptions.put("openApiNullable", "false")
  openapiNormalizer.put("REFACTOR_ALLOF_WITH_PROPERTIES_ONLY", "true")
  additionalProperties.put("apiNamePrefix", "PolarisCatalog")
  additionalProperties.put("apiNameSuffix", "")
  additionalProperties.put("metricsPrefix", "polaris")
  serverVariables.put("basePath", "api/catalog")
  importMappings =
    mapOf(
      "ErrorModel" to "org.apache.iceberg.rest.responses.ErrorResponse",
      "IcebergErrorResponse" to "org.apache.iceberg.rest.responses.ErrorResponse",
      "TableIdentifier" to "org.apache.iceberg.catalog.TableIdentifier",
    )
}

listOf("sourcesJar", "compileJava").forEach { task ->
  tasks.named(task) { dependsOn("openApiGenerate") }
}

sourceSets {
  main { java { srcDir(project.layout.buildDirectory.dir("generated/src/main/java")) } }
}

tasks.named("javadoc") { dependsOn("jandex") }

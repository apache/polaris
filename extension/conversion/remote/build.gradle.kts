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
  id("polaris-server")
  alias(libs.plugins.openapi.generator)
}

dependencies {
  implementation(project(":polaris-core"))
  implementation(project(":polaris-service-common"))
  implementation(project(":polaris-api-catalog-service"))

  implementation(libs.slf4j.api)

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.smallrye.common.annotation) // @Identifier
  compileOnly(libs.smallrye.config.core) // @ConfigMapping

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.fasterxml.jackson.core:jackson-core")
}

openApiGenerate {
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

description = "Implements table conversion via remote service"

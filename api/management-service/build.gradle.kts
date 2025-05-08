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

listOf("sourcesJar", "compileJava").forEach { task ->
  tasks.named(task) { dependsOn("openApiGenerate") }
}

sourceSets {
  main { java { srcDir(project.layout.buildDirectory.dir("generated/src/main/java")) } }
}

tasks.named("javadoc") { dependsOn("jandex") }

tasks.named("processResources") { dependsOn("openApiGenerate") }

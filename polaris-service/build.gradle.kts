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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.openapitools.generator.gradle.plugin.tasks.GenerateTask

plugins {
  alias(libs.plugins.shadow)
  alias(libs.plugins.openapi.generator)
  id("polaris-server")
  id("polaris-license-report")
  id("application")
}

dependencies {
  implementation(project(":polaris-core"))

  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")
  implementation("org.apache.iceberg:iceberg-aws")

  implementation(platform(libs.dropwizard.bom))
  implementation("io.dropwizard:dropwizard-core")
  implementation("io.dropwizard:dropwizard-auth")
  implementation("io.dropwizard:dropwizard-json-logging")

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-xml")
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  implementation(platform(libs.opentelemetry.bom))
  implementation("io.opentelemetry:opentelemetry-api")
  implementation("io.opentelemetry:opentelemetry-sdk-trace")
  implementation("io.opentelemetry:opentelemetry-exporter-logging")
  implementation(libs.opentelemetry.semconv)

  implementation(libs.caffeine)
  implementation(libs.guava)
  implementation(libs.slf4j.api)

  implementation(libs.prometheus.metrics.exporter.servlet.jakarta)
  implementation(platform(libs.micrometer.bom))
  implementation("io.micrometer:micrometer-core")
  implementation("io.micrometer:micrometer-registry-prometheus")

  compileOnly(libs.swagger.annotations)
  compileOnly(libs.jetbrains.annotations)
  compileOnly(libs.spotbugs.annotations)
  implementation(libs.swagger.jaxrs)
  implementation(libs.javax.annotation.api)

  implementation(libs.hadoop.client.api)

  implementation(libs.auth0.jwt)

  implementation(libs.logback.core)
  implementation(libs.bouncycastle.bcprov)

  compileOnly(libs.jetbrains.annotations)
  compileOnly(libs.spotbugs.annotations)

  implementation(platform(libs.google.cloud.storage.bom))
  implementation("com.google.cloud:google-cloud-storage")
  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:sts")
  implementation("software.amazon.awssdk:sts")
  implementation("software.amazon.awssdk:iam-policy-builder")
  implementation("software.amazon.awssdk:s3")

  testImplementation("org.apache.iceberg:iceberg-api:${libs.versions.iceberg.get()}:tests")
  testImplementation("org.apache.iceberg:iceberg-core:${libs.versions.iceberg.get()}:tests")
  testImplementation("io.dropwizard:dropwizard-testing")
  testImplementation(platform(libs.testcontainers.bom))
  testImplementation("org.testcontainers:testcontainers")
  testImplementation(libs.s3mock.testcontainers)

  testImplementation("org.apache.iceberg:iceberg-spark-3.5_2.12")
  testImplementation("org.apache.iceberg:iceberg-spark-extensions-3.5_2.12")
  testImplementation("org.apache.spark:spark-sql_2.12:3.5.1") {
    // exclude log4j dependencies
    exclude("org.apache.logging.log4j", "log4j-slf4j2-impl")
    exclude("org.apache.logging.log4j", "log4j-api")
    exclude("org.apache.logging.log4j", "log4j-1.2-api")
  }

  testImplementation("software.amazon.awssdk:glue")
  testImplementation("software.amazon.awssdk:kms")
  testImplementation("software.amazon.awssdk:dynamodb")

  testImplementation(platform(libs.junit.bom))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation(libs.assertj.core)
  testImplementation(libs.mockito.core)
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

if (project.properties.get("eclipseLink") == "true") {
  dependencies { implementation(project(":polaris-eclipselink")) }
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

tasks.register<GenerateTask>("generatePolarisService").configure {
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

  doFirst { delete(outputDir.get()) }
}

tasks.named("compileJava").configure { dependsOn("openApiGenerate", "generatePolarisService") }

sourceSets {
  main { java { srcDir(project.layout.buildDirectory.dir("generated/src/main/java")) } }
}

tasks.named<Test>("test").configure {
  if (System.getenv("AWS_REGION") == null) {
    environment("AWS_REGION", "us-west-2")
  }
  jvmArgs("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED")
  useJUnitPlatform()
  maxParallelForks = 4
}

tasks.register<JavaExec>("runApp").configure {
  if (System.getenv("AWS_REGION") == null) {
    environment("AWS_REGION", "us-west-2")
  }
  classpath = sourceSets["main"].runtimeClasspath
  mainClass = "org.apache.polaris.service.PolarisApplication"
  args("server", "$rootDir/polaris-server.yml")
}

application { mainClass = "org.apache.polaris.service.PolarisApplication" }

tasks.named<Jar>("jar") {
  manifest { attributes["Main-Class"] = "org.apache.polaris.service.PolarisApplication" }
}

tasks.named<ShadowJar>("shadowJar") {
  manifest { attributes["Main-Class"] = "org.apache.polaris.service.PolarisApplication" }
  archiveVersion.set("")
  mergeServiceFiles()
  isZip64 = true
}

tasks.named<CreateStartScripts>("startScripts") { classpath = files("polaris-service-all.jar") }

tasks.named("build").configure { dependsOn("shadowJar") }

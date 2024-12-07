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
  id("java-library")
  id("java-test-fixtures")
}

dependencies {
  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")
  constraints {
    implementation("io.airlift:aircompressor:0.27") { because("Vulnerability detected in 0.25") }
  }

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation(libs.caffeine)
  implementation(libs.commons.lang3)
  implementation(libs.commons.codec1)
  implementation(libs.guava)
  implementation(libs.slf4j.api)
  compileOnly(libs.jetbrains.annotations)
  compileOnly(libs.spotbugs.annotations)

  constraints {
    implementation("org.xerial.snappy:snappy-java:1.1.10.4") {
      because("Vulnerability detected in 1.1.8.2")
    }
    implementation("org.codehaus.jettison:jettison:1.5.4") {
      because("Vulnerability detected in 1.1")
    }
    implementation("org.apache.commons:commons-configuration2:2.10.1") {
      because("Vulnerability detected in 2.8.0")
    }
    implementation("org.apache.commons:commons-compress:1.26.0") {
      because("Vulnerability detected in 1.21")
    }
    implementation("com.nimbusds:nimbus-jose-jwt:9.37.2") {
      because("Vulnerability detected in 9.8.1")
    }
  }

  implementation(libs.javax.inject)
  implementation(libs.swagger.annotations)
  implementation(libs.swagger.jaxrs)
  implementation(libs.jakarta.validation.api)

  implementation("org.apache.iceberg:iceberg-aws")
  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:sts")
  implementation("software.amazon.awssdk:iam-policy-builder")
  implementation("software.amazon.awssdk:s3")

  implementation("org.apache.iceberg:iceberg-azure")
  implementation(platform(libs.azuresdk.bom))
  implementation("com.azure:azure-storage-blob")
  implementation("com.azure:azure-storage-common")
  implementation("com.azure:azure-identity")
  implementation("com.azure:azure-storage-file-datalake")
  constraints {
    implementation("io.netty:netty-codec-http2:4.1.100") {
      because("Vulnerability detected in 4.1.72")
    }
    implementation("io.projectreactor.netty:reactor-netty-http:1.1.13") {
      because("Vulnerability detected in 1.0.45")
    }
  }

  implementation("org.apache.iceberg:iceberg-gcp")
  implementation(platform(libs.google.cloud.storage.bom))
  implementation("com.google.cloud:google-cloud-storage")

  implementation(platform(libs.micrometer.bom))
  implementation("io.micrometer:micrometer-core")

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi("org.junit.jupiter:junit-jupiter")
  testFixturesApi(libs.assertj.core)
  testFixturesApi(libs.mockito.core)
  testFixturesApi("com.fasterxml.jackson.core:jackson-core")
  testFixturesApi("com.fasterxml.jackson.core:jackson-databind")
  testFixturesApi(libs.commons.lang3)
  testFixturesApi(libs.threeten.extra)
  testFixturesApi(platform(libs.jackson.bom))
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
  testFixturesApi(libs.jakarta.annotation.api)

  compileOnly(libs.jakarta.annotation.api)
}

openApiValidate { inputSpec = "$rootDir/spec/polaris-management-service.yml" }

val generatePolarisService by
  tasks.registering(GenerateTask::class) {
    inputSpec = "$rootDir/spec/polaris-management-service.yml"
    generatorName = "jaxrs-resteasy"
    outputDir = "$projectDir/build/generated"
    modelPackage = "org.apache.polaris.core.admin.model"
    ignoreFileOverride = "$rootDir/.openapi-generator-ignore"
    removeOperationIdPrefix = true
    templateDir = "$rootDir/server-templates"
    globalProperties.put("apis", "false")
    globalProperties.put("models", "")
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
    serverVariables = mapOf("basePath" to "api/v1")
  }

listOf("sourcesJar", "compileJava").forEach { task ->
  tasks.named(task) { dependsOn(generatePolarisService) }
}

sourceSets {
  main { java { srcDir(project.layout.buildDirectory.dir("generated/src/main/java")) } }
}

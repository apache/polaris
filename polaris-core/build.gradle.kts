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
  id("polaris-client")
  id("org.kordamp.gradle.jandex")
}

dependencies {
  implementation(project(":polaris-api-management-model"))

  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-smile")
  runtimeOnly("com.fasterxml.jackson.datatype:jackson-datatype-guava")
  runtimeOnly("com.fasterxml.jackson.datatype:jackson-datatype-jdk8")
  runtimeOnly("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

  implementation(libs.caffeine)
  implementation(libs.guava)
  implementation(libs.slf4j.api)

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  implementation(libs.swagger.annotations)
  implementation(libs.swagger.jaxrs)
  implementation(libs.jakarta.inject.api)
  implementation(libs.jakarta.validation.api)
  implementation(libs.jakarta.ws.rs.api)
  implementation(libs.smallrye.common.annotation)

  implementation("org.apache.iceberg:iceberg-aws")
  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:sts")
  implementation("software.amazon.awssdk:iam-policy-builder")
  implementation("software.amazon.awssdk:s3")
  implementation("software.amazon.awssdk:kms")

  implementation("org.apache.iceberg:iceberg-azure")
  implementation(platform(libs.azuresdk.bom))
  implementation("com.azure:azure-storage-blob")
  implementation("com.azure:azure-storage-common")
  implementation("com.azure:azure-identity")
  implementation("com.azure:azure-storage-file-datalake")

  implementation("org.apache.iceberg:iceberg-gcp")
  implementation(platform(libs.google.cloud.storage.bom))
  implementation("com.google.cloud:google-cloud-storage")

  testCompileOnly(project(":polaris-immutables"))
  testAnnotationProcessor(project(":polaris-immutables", configuration = "processor"))

  testFixturesApi("com.fasterxml.jackson.core:jackson-core")
  testFixturesApi("com.fasterxml.jackson.core:jackson-databind")
  testFixturesApi(libs.commons.lang3)
  testFixturesApi(libs.threeten.extra)
  testFixturesApi(platform(libs.jackson.bom))
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
  testFixturesApi(libs.jakarta.annotation.api)
  testFixturesApi(libs.jakarta.ws.rs.api)

  compileOnly(libs.jakarta.annotation.api)
}

tasks.named("javadoc") { dependsOn("jandex") }

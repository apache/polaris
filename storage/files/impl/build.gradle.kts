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
  id("org.kordamp.gradle.jandex")
}

dependencies {
  implementation(project(":polaris-storage-files-api"))

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

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  implementation(libs.jakarta.inject.api)
  implementation(libs.jakarta.validation.api)
  implementation(libs.jakarta.enterprise.cdi.api)

  implementation(platform(libs.nessie.bom))
  implementation("org.projectnessie.nessie:nessie-storage-uri")

  runtimeOnly("org.apache.iceberg:iceberg-aws")
  runtimeOnly(platform(libs.awssdk.bom))
  runtimeOnly("software.amazon.awssdk:sts")
  runtimeOnly("software.amazon.awssdk:iam-policy-builder")
  runtimeOnly("software.amazon.awssdk:s3")
  runtimeOnly("software.amazon.awssdk:kms")

  runtimeOnly("org.apache.iceberg:iceberg-azure")
  runtimeOnly(platform(libs.azuresdk.bom))
  runtimeOnly("com.azure:azure-storage-blob")
  runtimeOnly("com.azure:azure-storage-common")
  runtimeOnly("com.azure:azure-identity")
  runtimeOnly("com.azure:azure-storage-file-datalake")

  runtimeOnly("org.apache.iceberg:iceberg-gcp")
  runtimeOnly(platform(libs.google.cloud.storage.bom))
  runtimeOnly("com.google.cloud:google-cloud-storage")

  testFixturesApi(project(":polaris-storage-files-api"))

  testFixturesApi(platform(libs.nessie.bom))
  testImplementation("org.projectnessie.nessie:nessie-object-storage-mock")
  testFixturesApi("org.projectnessie.nessie:nessie-catalog-format-iceberg")

  testFixturesApi("com.fasterxml.jackson.core:jackson-core")
  testFixturesApi("com.fasterxml.jackson.core:jackson-databind")
  testFixturesApi(platform(libs.jackson.bom))
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")

  testFixturesApi(platform(libs.iceberg.bom))
  testFixturesApi("org.apache.iceberg:iceberg-api")
  testFixturesApi("org.apache.iceberg:iceberg-core")
  testFixturesApi("org.apache.iceberg:iceberg-aws")
  testFixturesApi("org.apache.iceberg:iceberg-azure")
  testFixturesApi("org.apache.iceberg:iceberg-gcp")

  testFixturesRuntimeOnly("software.amazon.awssdk:url-connection-client")

  compileOnly(libs.jakarta.annotation.api)
}

tasks.named("javadoc") { dependsOn("jandex") }

tasks.withType<Javadoc> {
  isFailOnError = false
  options.memberLevel = JavadocMemberLevel.PACKAGE
}

testing {
  suites {
    val intTest by
      registering(JvmTestSuite::class) {
        dependencies {
          implementation(project(":polaris-storage-files-api"))

          implementation("org.projectnessie.nessie:nessie-azurite-testcontainer")
          implementation("org.projectnessie.nessie:nessie-gcs-testcontainer")
          implementation(project(":polaris-minio-testcontainer"))

          implementation("org.apache.iceberg:iceberg-aws")
          implementation("org.apache.iceberg:iceberg-gcp")
          implementation("org.apache.iceberg:iceberg-azure")
        }
      }
  }
}

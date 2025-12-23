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

plugins { id("polaris-client") }

dependencies {
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.guava)

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.fasterxml.jackson.core:jackson-databind")
  compileOnly("com.fasterxml.jackson.dataformat:jackson-dataformat-smile")
  compileOnly("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml")

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  compileOnly(project(":polaris-api-management-model"))

  compileOnly(platform(libs.iceberg.bom))
  compileOnly("org.apache.iceberg:iceberg-api")
  compileOnly("org.apache.iceberg:iceberg-core")
  compileOnly("org.apache.iceberg:iceberg-aws")
  compileOnly("org.apache.iceberg:iceberg-azure")
  compileOnly("org.apache.iceberg:iceberg-gcp")

  compileOnly(platform(libs.awssdk.bom))
  compileOnly("software.amazon.awssdk:sts")
  compileOnly("software.amazon.awssdk:iam-policy-builder")
  compileOnly("software.amazon.awssdk:s3")
  compileOnly("software.amazon.awssdk:kms")

  compileOnly(platform(libs.azuresdk.bom))
  compileOnly("com.azure:azure-storage-blob")
  compileOnly("com.azure:azure-storage-common")
  compileOnly("com.azure:azure-identity")
  compileOnly("com.azure:azure-storage-file-datalake")

  compileOnly(platform(libs.google.cloud.storage.bom))
  compileOnly("com.google.cloud:google-cloud-storage")
  compileOnly(libs.google.cloud.iamcredentials)

  compileOnly(libs.caffeine)
  compileOnly(libs.slf4j.api)

  testCompileOnly(project(":polaris-immutables"))
  testAnnotationProcessor(project(":polaris-immutables", configuration = "processor"))

  testImplementation(platform(libs.jackson.bom))
  testImplementation("com.fasterxml.jackson.core:jackson-databind")
  testImplementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml")
  testImplementation(libs.guava)
}

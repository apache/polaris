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

plugins { id("polaris-server") }

dependencies {
  implementation(project(":polaris-core"))
  implementation(project(":polaris-api-management-model"))
  implementation(project(":polaris-api-management-service"))
  implementation(project(":polaris-api-iceberg-service"))

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

  testImplementation(platform(libs.junit.bom))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation(libs.assertj.core)
  testImplementation(libs.mockito.core)
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

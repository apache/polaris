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

  implementation(libs.jakarta.ws.rs.api)
  implementation(libs.guava)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.jakarta.rs:jackson-jakarta-rs-json-provider")

  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")

  implementation("org.apache.iceberg:iceberg-api:${libs.versions.iceberg.get()}:tests")
  implementation("org.apache.iceberg:iceberg-core:${libs.versions.iceberg.get()}:tests")

  implementation(libs.hadoop.common) {
    exclude("org.slf4j", "slf4j-reload4j")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("ch.qos.reload4j", "reload4j")
    exclude("log4j", "log4j")
    exclude("org.apache.zookeeper", "zookeeper")
  }

  implementation(libs.auth0.jwt)

  implementation(platform(libs.testcontainers.bom))
  implementation("org.testcontainers:testcontainers")
  implementation(libs.s3mock.testcontainers)

  implementation("org.apache.iceberg:iceberg-spark-3.5_2.12")
  implementation("org.apache.iceberg:iceberg-spark-extensions-3.5_2.12")
  implementation("org.apache.spark:spark-sql_2.12:3.5.5") {
    // exclude log4j dependencies
    exclude("org.apache.logging.log4j", "log4j-slf4j2-impl")
    exclude("org.apache.logging.log4j", "log4j-api")
    exclude("org.apache.logging.log4j", "log4j-1.2-api")
    exclude("org.slf4j", "jul-to-slf4j")
  }

  implementation(platform(libs.junit.bom))
  implementation("org.junit.jupiter:junit-jupiter")
  implementation("org.junit.jupiter:junit-jupiter-api")
  compileOnly("org.junit.jupiter:junit-jupiter-engine")
  implementation(libs.assertj.core)
  implementation(libs.mockito.core)
}

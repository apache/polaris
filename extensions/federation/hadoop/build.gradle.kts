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
  alias(libs.plugins.jandex)
}

dependencies {
  // Polaris dependencies
  implementation(project(":polaris-core"))

  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")
  implementation("org.apache.iceberg:iceberg-common")

  // Hadoop dependencies (for Hadoop catalog support)
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
    exclude("io.dropwizard.metrics", "metrics-core")
  }
  implementation(libs.hadoop.client.api)
  implementation(libs.hadoop.client.runtime)

  // CDI dependencies for runtime discovery
  implementation(libs.jakarta.enterprise.cdi.api)
  implementation(libs.smallrye.common.annotation)

  // Logging
  implementation(libs.slf4j.api)
}

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
  implementation(project(":polaris-core"))

  implementation(fileTree("override-libs") { include("*.jar") })

  implementation("org.apache.ranger:ranger-plugins-common:2.8.0")

  implementation("org.apache.ranger:authz-embedded:2.8.0")

  // Iceberg dependency for ForbiddenException
  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation(project(":polaris-async-api"))
  implementation(libs.guava)

  compileOnly(project(":polaris-immutables"))
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.smallrye.config.core)

  implementation("org.apache.commons:commons-lang3:3.19.0")
  runtimeOnly("org.apache.commons:commons-configuration2:2.10.1")
  runtimeOnly("org.apache.commons:commons-text")
  runtimeOnly("commons-collections:commons-collections:3.2.2")
  runtimeOnly("org.apache.ranger:ranger-audit-core:2.8.0")
  runtimeOnly("org.apache.ranger:ranger-plugin-classloader:2.8.0")
  runtimeOnly("org.apache.ranger:ranger-plugins-cred:2.8.0")
  runtimeOnly("org.apache.ranger:ranger-audit-dest-solr:2.8.0")
  runtimeOnly("org.apache.zookeeper:zookeeper:3.8.4")
}

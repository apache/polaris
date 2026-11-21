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

description = "Apache Polaris - Catalog API common classes"

// This module is shared between the Polaris server and clients (e.g. the Spark plugin), and is
// bundled into client jars. It must stay thin: Iceberg and SLF4J are provided by the runtime
// environment (Spark / Polaris server), so they must not become transitive dependencies.
dependencies {
  compileOnly(platform(libs.iceberg.bom))
  compileOnly("org.apache.iceberg:iceberg-api")
  compileOnly("org.apache.iceberg:iceberg-core")
  compileOnly(libs.slf4j.api)

  testImplementation(platform(libs.iceberg.bom))
  testImplementation("org.apache.iceberg:iceberg-api")
  testImplementation("org.apache.iceberg:iceberg-core")
  testImplementation(platform(libs.junit.bom))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation(libs.assertj.core)
}

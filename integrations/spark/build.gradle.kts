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
    id("polaris-spark")
}

val sparkScala = prepareSparkScalaProject()

dependencies {
  compileOnly("org.apache.spark:spark-sql_${sparkScala.scalaMajorMinorVersion}") { sparkDependencyAndExcludes(sparkScala) }

  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-spark-runtime-${sparkScala.sparkMajorMinorVersion}_${sparkScala.scalaMajorMinorVersion}")

  implementation(project(":polaris-scala-demo_${sparkScala.scalaMajorMinorVersion}"))

  compileOnly("org.apache.iceberg:iceberg-spark-${sparkScala.sparkMajorMinorVersion}_${sparkScala.scalaMajorMinorVersion}")

  testImplementation("org.apache.spark:spark-sql_${sparkScala.scalaMajorMinorVersion}") { sparkDependencyAndExcludes(sparkScala) }
}

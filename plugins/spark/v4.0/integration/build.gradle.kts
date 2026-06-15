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
  alias(libs.plugins.quarkus)
  id("org.kordamp.gradle.jandex")
  id("polaris-runtime")
}

// get version information
val sparkMajorVersion = "4.0"
val scalaVersion = getAndUseScalaVersionForProject()
val icebergVersion = libs.versions.iceberg.get()
val spark40Version = libs.versions.spark40.get()
val scalaLibraryVersion = libs.versions.scala213.get()

sourceSets {
  named("intTest") {
    java { srcDir("../../common/src/intTest/java") }
    resources { srcDir("../../common/src/intTest/resources") }
  }
}

dependencies {
  // must be enforced to get a consistent and validated set of dependencies
  implementation(enforcedPlatform(libs.quarkus.bom)) {
    exclude(group = "org.antlr", module = "antlr4-runtime")
    exclude(group = "org.scala-lang", module = "scala-library")
    exclude(group = "org.scala-lang", module = "scala-reflect")
  }

  // For test configurations, exclude jakarta.servlet-api from Quarkus BOM
  // to allow Spark 4.0's version (5.0.0) which includes SingleThreadModel
  testImplementation(platform(libs.quarkus.bom)) {
    exclude(group = "jakarta.servlet", module = "jakarta.servlet-api")
  }

  implementation(project(":polaris-runtime-service"))

  testImplementation(
    "org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_${scalaVersion}:${icebergVersion}"
  )
  testImplementation(project(":polaris-spark-${sparkMajorVersion}_${scalaVersion}"))

  testImplementation(project(":polaris-api-management-model"))

  testImplementation(project(":polaris-runtime-test-common"))

  testImplementation("org.apache.spark:spark-sql_${scalaVersion}:${spark40Version}") {
    // exclude log4j dependencies. Explicit dependencies for the log4j libraries are
    // enforced below to ensure the version compatibility
    exclude("org.apache.logging.log4j", "log4j-slf4j2-impl")
    exclude("org.apache.logging.log4j", "log4j-1.2-api")
    exclude("org.apache.logging.log4j", "log4j-core")
    exclude("org.slf4j", "jul-to-slf4j")
  }

  // Add spark-hive for Hudi integration - provides HiveExternalCatalog that Hudi needs
  testRuntimeOnly("org.apache.spark:spark-hive_${scalaVersion}:${spark40Version}") {
    // exclude log4j dependencies to match spark-sql exclusions
    exclude("org.apache.logging.log4j", "log4j-slf4j2-impl")
    exclude("org.apache.logging.log4j", "log4j-1.2-api")
    exclude("org.apache.logging.log4j", "log4j-core")
    exclude("org.slf4j", "jul-to-slf4j")
    // exclude old slf4j 1.x to log4j 2.x bridge that conflicts with slf4j 2.x bridge
    exclude("org.apache.logging.log4j", "log4j-slf4j-impl")
  }
  // enforce the usage of log4j 2.24.3. This is for the log4j-api compatibility
  // of spark-sql dependency
  testRuntimeOnly("org.apache.logging.log4j:log4j-core:2.26.0")

  testImplementation("io.delta:delta-spark_4.0_${scalaVersion}:4.2.0")
  testImplementation("org.apache.hudi:hudi-spark4.0-bundle_${scalaVersion}:1.1.1") {
    // exclude log4j dependencies to match spark-sql exclusions
    // exclude log4j dependencies to match spark-sql exclusions and prevent version conflicts
    exclude("org.apache.logging.log4j", "log4j-slf4j2-impl")
    exclude("org.apache.logging.log4j", "log4j-1.2-api")
    exclude("org.apache.logging.log4j", "log4j-core")
    exclude("org.slf4j", "jul-to-slf4j")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("org.slf4j", "slf4j-reload4j")
    exclude("ch.qos.reload4j", "reload4j")
    exclude("log4j", "log4j")
    // exclude old slf4j 1.x to log4j 2.x bridge that conflicts with slf4j 2.x bridge
    exclude("org.apache.logging.log4j", "log4j-slf4j-impl")
  }

  // The hudi-spark-bundle includes most Hive libraries but excludes hive-exec to keep size
  // manageable
  // This matches what Spark 4.0 distribution provides (hive-exec-2.3.10-core.jar)
  testImplementation("org.apache.hive:hive-exec:2.3.10:core") {
    // Exclude conflicting dependencies to use Spark's versions
    exclude("org.apache.hadoop", "*")
    exclude("org.apache.commons", "*")
    exclude("org.slf4j", "*")
    exclude("log4j", "*")
    exclude("org.apache.logging.log4j", "*")
    exclude("org.pentaho", "*")
    exclude("org.apache.calcite", "*")
    exclude("org.apache.tez", "*")
  }

  testImplementation(platform(libs.jackson.bom))
  testImplementation("com.fasterxml.jackson.jakarta.rs:jackson-jakarta-rs-json-provider")

  testImplementation(testFixtures(project(":polaris-runtime-service")))

  testImplementation(platform(libs.quarkus.bom))
  testImplementation("io.quarkus:quarkus-junit")
  testImplementation("io.quarkus:quarkus-rest-client")
  testImplementation("io.quarkus:quarkus-rest-client-jackson")

  testImplementation(platform(libs.awssdk.bom))
  testImplementation("software.amazon.awssdk:glue")
  testImplementation("software.amazon.awssdk:kms")
  testImplementation("software.amazon.awssdk:dynamodb")

  testImplementation(platform(libs.testcontainers.bom))
  testImplementation("org.testcontainers:testcontainers")
  testImplementation(libs.s3mock.testcontainers)

  // Required for Spark integration tests
  testImplementation(enforcedPlatform("org.scala-lang:scala-library:${scalaLibraryVersion}"))
  testImplementation(enforcedPlatform("org.scala-lang:scala-reflect:${scalaLibraryVersion}"))
  testImplementation(libs.javax.servlet.api)
  testImplementation(libs.antlr4.runtime.spark40)
}

// Force jakarta.servlet-api to 5.0.0 for Spark 4.0 compatibility
// Spark 4.0 requires version 5.0.0 which includes SingleThreadModel
// Quarkus BOM forces it to 6.x which removed SingleThreadModel
configurations.named("intTestRuntimeClasspath") {
  resolutionStrategy { force("jakarta.servlet:jakarta.servlet-api:5.0.0") }
}

tasks.named<Test>("intTest").configure {
  if (System.getenv("AWS_REGION") == null) {
    environment("AWS_REGION", "us-west-2")
  }
  // Note: the test secrets are referenced in
  // org.apache.polaris.service.it.ServerManager
  environment("POLARIS_BOOTSTRAP_CREDENTIALS", "POLARIS,test-admin,test-secret")
  jvmArgs("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED")
  // Need to allow a java security manager after Java 21, for Subject.getSubject to work
  // "getSubject is supported only if a security manager is allowed".
  systemProperty("java.security.manager", "allow")
  // Same issue as above: allow a java security manager after Java 21
  // (this setting is for the application under test, while the setting above is for test code).
  systemProperty("quarkus.test.arg-line", "-Djava.security.manager=allow")
  val logsDir = project.layout.buildDirectory.get().asFile.resolve("logs")
  // delete files from previous runs
  doFirst {
    // delete log files written by Polaris
    logsDir.deleteRecursively()
    // delete quarkus.log file (captured Polaris stdout/stderr)
    project.layout.buildDirectory.get().asFile.resolve("quarkus.log").delete()
  }
  // This property is not honored in a per-profile application.properties file,
  // so we need to set it here.
  systemProperty("quarkus.log.file.path", logsDir.resolve("polaris.log").absolutePath)
  // For Spark integration tests
  addSparkJvmOptions()
}

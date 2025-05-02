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
  alias(libs.plugins.jandex)
  id("polaris-quarkus")
}

// get version information
val sparkMajorVersion = "3.5"
val scalaVersion = getAndUseScalaVersionForProject()
val icebergVersion = pluginlibs.versions.iceberg.get()
val spark35Version = pluginlibs.versions.spark35.get()
val scalaLibraryVersion =
  if (scalaVersion == "2.12") {
    pluginlibs.versions.scala212.get()
  } else {
    pluginlibs.versions.scala213.get()
  }

dependencies {
  // must be enforced to get a consistent and validated set of dependencies
  implementation(enforcedPlatform(libs.quarkus.bom)) {
    exclude(group = "org.antlr", module = "antlr4-runtime")
    exclude(group = "org.scala-lang", module = "scala-library")
    exclude(group = "org.scala-lang", module = "scala-reflect")
  }

  implementation(project(":polaris-quarkus-service"))

  testImplementation(project(":polaris-api-management-model"))
  testImplementation(project(":polaris-spark-${sparkMajorVersion}_${scalaVersion}"))

  testImplementation("org.apache.spark:spark-sql_${scalaVersion}:${spark35Version}") {
    // exclude log4j dependencies
    exclude("org.apache.logging.log4j", "log4j-slf4j2-impl")
    exclude("org.apache.logging.log4j", "log4j-api")
    exclude("org.apache.logging.log4j", "log4j-1.2-api")
    exclude("org.slf4j", "jul-to-slf4j")
  }

  testImplementation(platform(libs.jackson.bom))
  testImplementation("com.fasterxml.jackson.core:jackson-annotations")
  testImplementation("com.fasterxml.jackson.core:jackson-core")
  testImplementation("com.fasterxml.jackson.core:jackson-databind")

  testImplementation(
    "org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_${scalaVersion}:${icebergVersion}"
  )

  testImplementation(testFixtures(project(":polaris-quarkus-service")))

  testImplementation(platform(libs.quarkus.bom))
  testImplementation("io.quarkus:quarkus-junit5")
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
  testImplementation(libs.antlr4.runtime)
}

tasks.named<Test>("intTest").configure {
  maxParallelForks = 1
  systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
  if (System.getenv("AWS_REGION") == null) {
    environment("AWS_REGION", "us-west-2")
  }
  // Note: the test secrets are referenced in
  // org.apache.polaris.service.quarkus.it.QuarkusServerManager
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

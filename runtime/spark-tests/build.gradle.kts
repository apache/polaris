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

dependencies {

  // must be enforced to get a consistent and validated set of dependencies
  implementation(enforcedPlatform(libs.quarkus.bom)) {
    exclude(group = "org.antlr", module = "antlr4-runtime")
    exclude(group = "org.scala-lang", module = "scala-library")
    exclude(group = "org.scala-lang", module = "scala-reflect")
  }

  implementation(project(":polaris-runtime-service"))
  runtimeOnly(project(":polaris-extensions-federation-hive")) {
    // Brings shaded parquet 1.10 which conflicts with Iceberg's parquet 1.16 in test code.
    exclude("org.apache.parquet", "parquet-hadoop-bundle")
  }

  // Adds hadoop-aws to the Polaris JVM classpath so Hive federation's HiveCatalog can use
  // HadoopFileIO (its default) against an s3a:// warehouse.
  runtimeOnly(libs.hadoop.aws) {
    // Exclude the AWS SDK V2 fat-jar `bundle`: hadoop-aws 3.4.3 pulls bundle:2.35.4, which clashes
    // with Polaris's individual SDK modules. The required modules (s3, sts, etc.) are already on
    // the classpath via the awssdk-bom.
    exclude("software.amazon.awssdk", "bundle")
    exclude("org.slf4j", "slf4j-reload4j")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("ch.qos.reload4j", "reload4j")
    exclude("log4j", "log4j")
  }
  // hadoop-aws references s3-transfer-manager classes but doesn't declare a direct dependency
  // (the bundle fat-jar provided them); add it explicitly at the BOM-managed version.
  runtimeOnly("software.amazon.awssdk:s3-transfer-manager")

  testImplementation(project(":polaris-tests"))
  testImplementation(project(":polaris-rustfs-testcontainer"))
  testImplementation(testFixtures(project(":polaris-runtime-service")))
  testImplementation(project(":polaris-runtime-test-common"))

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
  testImplementation(enforcedPlatform(libs.scala212.lang.library))
  testImplementation(enforcedPlatform(libs.scala212.lang.reflect))
  testImplementation(libs.javax.servlet.api)
  testImplementation(libs.antlr4.runtime)
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

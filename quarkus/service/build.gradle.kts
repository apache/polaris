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

dependencies {
  implementation(project(":polaris-core"))
  implementation(project(":polaris-api-management-service"))
  implementation(project(":polaris-api-iceberg-service"))
  implementation(project(":polaris-service-common"))
  implementation(project(":polaris-quarkus-defaults"))

  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")
  implementation("org.apache.iceberg:iceberg-aws")

  implementation(platform(libs.opentelemetry.bom))

  implementation(platform(libs.quarkus.bom))
  implementation("io.quarkus:quarkus-logging-json")
  implementation("io.quarkus:quarkus-rest-jackson")
  implementation("io.quarkus:quarkus-reactive-routes")
  implementation("io.quarkus:quarkus-hibernate-validator")
  implementation("io.quarkus:quarkus-smallrye-health")
  implementation("io.quarkus:quarkus-micrometer")
  implementation("io.quarkus:quarkus-micrometer-registry-prometheus")
  implementation("io.quarkus:quarkus-opentelemetry")
  implementation("io.quarkus:quarkus-smallrye-context-propagation")

  implementation(libs.jakarta.enterprise.cdi.api)
  implementation(libs.jakarta.inject.api)
  implementation(libs.jakarta.validation.api)
  implementation(libs.jakarta.ws.rs.api)

  implementation(libs.caffeine)
  implementation(libs.guava)
  implementation(libs.slf4j.api)

  implementation("org.jboss.slf4j:slf4j-jboss-logmanager")

  implementation(libs.hadoop.client.api)
  implementation(libs.hadoop.client.runtime)

  implementation(libs.auth0.jwt)

  implementation(libs.bouncycastle.bcprov)

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.spotbugs.annotations)

  implementation(platform(libs.google.cloud.storage.bom))
  implementation("com.google.cloud:google-cloud-storage")
  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:sts")
  implementation("software.amazon.awssdk:iam-policy-builder")
  implementation("software.amazon.awssdk:s3")
  implementation(platform(libs.azuresdk.bom))
  implementation("com.azure:azure-core")

  compileOnly(libs.swagger.annotations)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")

  implementation(libs.jakarta.servlet.api)

  testFixturesApi(project(":polaris-tests")) {
    // exclude all spark dependencies
    exclude(group = "org.apache.iceberg", module = "iceberg-spark-3.5_2.12")
    exclude(group = "org.apache.iceberg", module = "iceberg-spark-extensions-3.5_2.12")
    exclude(group = "org.apache.spark", module = "spark-sql_2.12")
  }

  testImplementation(project(":polaris-api-management-model"))
  testImplementation(testFixtures(project(":polaris-service-common")))

  testImplementation("org.apache.iceberg:iceberg-api:${libs.versions.iceberg.get()}:tests")
  testImplementation("org.apache.iceberg:iceberg-core:${libs.versions.iceberg.get()}:tests")

  testImplementation("software.amazon.awssdk:glue")
  testImplementation("software.amazon.awssdk:kms")
  testImplementation("software.amazon.awssdk:dynamodb")

  testImplementation(platform(libs.quarkus.bom))
  testImplementation("io.quarkus:quarkus-junit5")
  testImplementation("io.quarkus:quarkus-junit5-mockito")
  testImplementation("io.quarkus:quarkus-rest-client")
  testImplementation("io.quarkus:quarkus-rest-client-jackson")
  testImplementation("io.rest-assured:rest-assured")

  testImplementation(libs.threeten.extra)
  testImplementation(libs.hawkular.agent.prometheus.scraper)
}

tasks.withType(Test::class.java).configureEach {
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
}

tasks.named<Test>("test").configure { maxParallelForks = 4 }

tasks.named<Test>("intTest").configure {
  maxParallelForks = 1
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
}

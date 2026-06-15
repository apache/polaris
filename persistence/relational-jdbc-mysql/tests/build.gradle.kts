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

// Mirror `runtime/test-common`'s antlr/Scala exclusions and additionally drop
// jackson-module-scala: Jackson's ServiceLoader otherwise tries to instantiate
// DefaultScalaModule during RESTEasy provider init and fails.
configurations.all {
  if (name != "checkstyle") {
    exclude(group = "org.antlr", module = "antlr4-runtime")
    exclude(group = "org.scala-lang", module = "scala-library")
    exclude(group = "org.scala-lang", module = "scala-reflect")
    exclude(group = "com.fasterxml.jackson.module", module = "jackson-module-scala_2.12")
    exclude(group = "com.fasterxml.jackson.module", module = "jackson-module-scala_2.13")
  }
}

dependencies {
  implementation(platform(libs.quarkus.bom))
  implementation("io.quarkus:quarkus-rest-jackson")
  // The MySQL JDBC driver lives only in this test module so it stays off the
  // production-facing `runtime/service` classpath.
  implementation("io.quarkus:quarkus-jdbc-mysql")
  implementation(project(":polaris-relational-jdbc"))
  implementation(project(":polaris-runtime-service"))

  testImplementation(project(":polaris-runtime-test-common"))

  intTestImplementation("io.quarkus:quarkus-junit")
  intTestImplementation("io.rest-assured:rest-assured")
  intTestImplementation(project(":polaris-api-management-model"))
  intTestImplementation(project(":polaris-tests"))
  // Reuse the existing `ServerManager` (the PolarisServerManager SPI impl) from
  // runtime-service test fixtures instead of duplicating it in this module.
  intTestImplementation(testFixtures(project(":polaris-runtime-service")))
  intTestImplementation(platform(libs.iceberg.bom))
  intTestImplementation("org.apache.iceberg:iceberg-api")
  intTestImplementation("org.apache.iceberg:iceberg-core")
  // CatalogTests / ViewCatalogTests test fixtures used by Mysql*IT base classes.
  intTestImplementation("org.apache.iceberg:iceberg-api:${libs.versions.iceberg.get()}:tests")
  intTestImplementation("org.apache.iceberg:iceberg-core:${libs.versions.iceberg.get()}:tests")

  intTestImplementation(platform(libs.testcontainers.bom))
  intTestImplementation("org.testcontainers:testcontainers-junit-jupiter")
  intTestImplementation("org.testcontainers:testcontainers-mysql")
  intTestImplementation(project(":polaris-container-spec-helper"))

  // RESTEasy Classic (via keycloak-admin-client) provides the jakarta.ws.rs.client.ClientBuilder
  // SPI used by `PolarisClient`. Must remain `intTestRuntimeOnly` so it does not participate
  // in Quarkus augmentation (which uses RESTEasy Reactive).
  intTestRuntimeOnly(libs.keycloak.admin.client)
}

// `runtime/defaults` keeps the MySQL named datasource off by default; flip it on at Quarkus
// build time for this test module only. The matching `active=true` runtime override is set
// by `MysqlRelationalJdbcLifeCycleManagement`.
quarkus { quarkusBuildProperties.put("quarkus.datasource.mysql.jdbc", "true") }

tasks.named("javadoc") { dependsOn("jandex") }

tasks.withType<Test> {
  if (System.getenv("AWS_REGION") == null) {
    environment("AWS_REGION", "us-west-2")
  }
  environment("POLARIS_BOOTSTRAP_CREDENTIALS", "POLARIS,test-admin,test-secret")
  val apiVersion = System.getenv("DOCKER_API_VERSION") ?: "1.44"
  systemProperty("api.version", apiVersion)
  jvmArgs("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED")
  systemProperty("java.security.manager", "allow")
  maxParallelForks = 1

  val logsDir = project.layout.buildDirectory.get().asFile.resolve("logs")
  jvmArgumentProviders.add(
    CommandLineArgumentProvider {
      listOf("-Dquarkus.log.file.path=${logsDir.resolve("polaris.log").absolutePath}")
    }
  )
  doFirst {
    logsDir.deleteRecursively()
    project.layout.buildDirectory.get().asFile.resolve("quarkus.log").delete()
  }
}

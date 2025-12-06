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
  id("java-test-fixtures")
}

dependencies {
  implementation(project(":polaris-core"))
  implementation(project(":polaris-api-management-model"))
  implementation(project(":polaris-api-management-service"))
  implementation(project(":polaris-api-iceberg-service"))
  implementation(project(":polaris-api-catalog-service"))

  runtimeOnly(project(":polaris-relational-jdbc"))

  implementation(project(":polaris-runtime-defaults"))
  implementation(project(":polaris-runtime-common"))

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

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
  implementation("io.quarkus:quarkus-oidc")
  implementation("io.quarkus:quarkus-opentelemetry")
  implementation("io.quarkus:quarkus-security")
  implementation("io.quarkus:quarkus-smallrye-context-propagation")
  implementation("io.quarkus:quarkus-smallrye-fault-tolerance")
  runtimeOnly("io.quarkus:quarkus-jdbc-postgresql")

  implementation(libs.jakarta.enterprise.cdi.api)
  implementation(libs.jakarta.inject.api)
  implementation(libs.jakarta.validation.api)
  implementation(libs.jakarta.ws.rs.api)

  implementation(libs.caffeine)
  implementation(libs.guava)
  implementation(libs.slf4j.api)

  implementation(libs.hadoop.client.api)
  implementation(libs.hadoop.client.runtime)

  implementation(libs.auth0.jwt)

  implementation(libs.smallrye.common.annotation)
  implementation(libs.swagger.jaxrs)
  implementation(libs.microprofile.fault.tolerance.api)

  compileOnly(libs.jakarta.annotation.api)

  implementation(platform(libs.google.cloud.storage.bom))
  implementation("com.google.cloud:google-cloud-storage")
  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:sts")
  implementation("software.amazon.awssdk:iam-policy-builder")
  implementation("software.amazon.awssdk:s3")
  implementation("software.amazon.awssdk:kms")
  implementation("software.amazon.awssdk:cloudwatchlogs")
  implementation("software.amazon.awssdk:apache-client") {
    exclude("commons-logging", "commons-logging")
  }
  implementation(platform(libs.azuresdk.bom))
  implementation("com.azure:azure-core")
  implementation("com.azure:azure-storage-blob")
  implementation("com.azure:azure-storage-file-datalake")

  compileOnly(libs.swagger.annotations)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")

  implementation(libs.jakarta.servlet.api)

  runtimeOnly(project(":polaris-async-vertx"))

  testFixturesApi(project(":polaris-tests")) {
    // exclude all spark dependencies
    exclude(group = "org.apache.iceberg", module = "iceberg-spark-3.5_2.12")
    exclude(group = "org.apache.iceberg", module = "iceberg-spark-extensions-3.5_2.12")
    exclude(group = "org.apache.spark", module = "spark-sql_2.12")
  }

  testImplementation(project(":polaris-api-management-model"))
  testImplementation(project(":polaris-relational-jdbc"))

  testImplementation(project(":polaris-minio-testcontainer"))

  testImplementation("org.apache.iceberg:iceberg-api:${libs.versions.iceberg.get()}:tests")
  testImplementation("org.apache.iceberg:iceberg-core:${libs.versions.iceberg.get()}:tests")

  testImplementation("software.amazon.awssdk:glue")
  testImplementation("software.amazon.awssdk:kms")
  testImplementation("software.amazon.awssdk:dynamodb")

  testImplementation(enforcedPlatform(libs.quarkus.bom))
  testImplementation("io.quarkus:quarkus-junit5")
  testImplementation("io.quarkus:quarkus-junit5-mockito")
  testImplementation("io.quarkus:quarkus-rest-client")
  testImplementation("io.quarkus:quarkus-rest-client-jackson")
  testImplementation("io.quarkus:quarkus-jdbc-h2")

  testImplementation("io.opentelemetry:opentelemetry-sdk-testing")

  testImplementation("io.rest-assured:rest-assured")

  testImplementation(platform(libs.testcontainers.bom))
  testImplementation("org.testcontainers:testcontainers-localstack")

  testImplementation(project(":polaris-runtime-test-common"))
  testImplementation(project(":polaris-container-spec-helper"))

  testImplementation(libs.threeten.extra)
  testImplementation(libs.hawkular.agent.prometheus.scraper)

  testImplementation(libs.awaitility)

  testImplementation(platform(libs.testcontainers.bom))
  testImplementation("org.testcontainers:testcontainers")
  testImplementation("org.testcontainers:testcontainers-postgresql")

  testFixturesImplementation(project(":polaris-core"))
  testFixturesImplementation(project(":polaris-api-management-model"))
  testFixturesImplementation(project(":polaris-api-management-service"))
  testFixturesImplementation(project(":polaris-api-iceberg-service"))
  testFixturesImplementation(project(":polaris-api-catalog-service"))

  testFixturesImplementation(libs.jakarta.enterprise.cdi.api)
  testFixturesImplementation(libs.jakarta.annotation.api)
  testFixturesImplementation(libs.jakarta.ws.rs.api)

  testFixturesImplementation(platform(libs.quarkus.bom))
  testFixturesImplementation("io.quarkus:quarkus-rest-client")
  testFixturesImplementation("io.quarkus:quarkus-rest-client-jackson")

  testFixturesImplementation(platform(libs.iceberg.bom))
  testFixturesImplementation("org.apache.iceberg:iceberg-api")
  testFixturesImplementation("org.apache.iceberg:iceberg-core")
  testFixturesImplementation("org.apache.iceberg:iceberg-aws")

  testFixturesImplementation(platform(libs.google.cloud.storage.bom))
  testFixturesImplementation("com.google.cloud:google-cloud-storage")
  testFixturesImplementation(platform(libs.awssdk.bom))
  testFixturesImplementation("software.amazon.awssdk:sts")
  testFixturesImplementation("software.amazon.awssdk:iam-policy-builder")
  testFixturesImplementation("software.amazon.awssdk:s3")
  testFixturesImplementation("software.amazon.awssdk:kms")

  testFixturesImplementation(platform(libs.azuresdk.bom))
  testFixturesImplementation("com.azure:azure-core")
  testFixturesImplementation("com.azure:azure-storage-blob")
  testFixturesImplementation("com.azure:azure-storage-file-datalake")

  // This dependency brings in RESTEasy Classic, which conflicts with Quarkus RESTEasy Reactive;
  // it must not be present during Quarkus augmentation otherwise Quarkus tests won't start.
  intTestRuntimeOnly(libs.keycloak.admin.client)
}

tasks.named("javadoc") { dependsOn("jandex") }

tasks.withType(Test::class.java).configureEach {
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
}

listOf("intTest", "cloudTest")
  .map { tasks.named<Test>(it) }
  .forEach {
    it.configure {
      maxParallelForks = 1

      val logsDir = project.layout.buildDirectory.get().asFile.resolve("logs")

      // JVM arguments provider does not interfere with Gradle's cache keys
      jvmArgumentProviders.add(
        CommandLineArgumentProvider {
          // Same issue as above: allow a java security manager after Java 21
          // (this setting is for the application under test, while the setting above is for test
          // code).
          val securityManagerAllow = "-Djava.security.manager=allow"

          val args = mutableListOf<String>()

          // Example: to attach a debugger to the spawned JVM running Quarkus, add
          // -Dquarkus.test.arg-line=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005
          // to your test configuration.
          val explicitQuarkusTestArgLine = System.getProperty("quarkus.test.arg-line")
          var quarkusTestArgLine =
            if (explicitQuarkusTestArgLine != null)
              "$explicitQuarkusTestArgLine $securityManagerAllow"
            else securityManagerAllow

          args.add("-Dquarkus.test.arg-line=$quarkusTestArgLine")
          // This property is not honored in a per-profile application.properties file,
          // so we need to set it here.
          args.add("-Dquarkus.log.file.path=${logsDir.resolve("polaris.log").absolutePath}")

          // Add `quarkus.*` system properties, other than the ones explicitly set above
          System.getProperties()
            .filter {
              it.key.toString().startsWith("quarkus.") &&
                !"quarkus.test.arg-line".equals(it.key) &&
                !"quarkus.log.file.path".equals(it.key)
            }
            .forEach { args.add("${it.key}=${it.value}") }

          args
        }
      )
      // delete files from previous runs
      doFirst {
        // delete log files written by Polaris
        logsDir.deleteRecursively()
        // delete quarkus.log file (captured Polaris stdout/stderr)
        project.layout.buildDirectory.get().asFile.resolve("quarkus.log").delete()
      }
    }
  }

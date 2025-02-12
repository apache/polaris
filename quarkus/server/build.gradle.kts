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

import io.quarkus.gradle.tasks.QuarkusRun

plugins {
  alias(libs.plugins.quarkus)
  alias(libs.plugins.jandex)
  alias(libs.plugins.openapi.generator)
  id("polaris-quarkus")
  // id("polaris-license-report")
  id("distribution")
}

dependencies {
  implementation(project(":polaris-core"))
  implementation(project(":polaris-api-management-service"))
  implementation(project(":polaris-api-iceberg-service"))
  implementation(project(":polaris-service-common"))
  implementation(project(":polaris-quarkus-defaults"))

  if (project.hasProperty("eclipseLinkDeps")) {
    runtimeOnly(project(":polaris-eclipselink"))
  }

  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")
  implementation("org.apache.iceberg:iceberg-aws")

  implementation(platform(libs.opentelemetry.bom))

  // enforce the Quarkus _platform_ here, to get a consistent and validated set of dependencies
  implementation(enforcedPlatform(libs.quarkus.bom)) {
    // Spark IT requires older versions of Scala and Antlr
    exclude("org.scala-lang", "scala-library")
    exclude("org.scala-lang", "scala-reflect")
    exclude("org.antlr", "antlr4-runtime")
  }
  implementation("io.quarkus:quarkus-logging-json")
  implementation("io.quarkus:quarkus-rest-jackson")
  implementation("io.quarkus:quarkus-reactive-routes")
  implementation("io.quarkus:quarkus-hibernate-validator")
  implementation("io.quarkus:quarkus-smallrye-health")
  implementation("io.quarkus:quarkus-micrometer")
  implementation("io.quarkus:quarkus-micrometer-registry-prometheus")
  implementation("io.quarkus:quarkus-opentelemetry")
  implementation("io.quarkus:quarkus-smallrye-context-propagation")
  implementation("io.quarkus:quarkus-container-image-docker")

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

  testFixturesApi(project(":polaris-tests"))

  testImplementation(project(":polaris-api-management-model"))
  testImplementation(testFixtures(project(":polaris-service-common")))

  testImplementation("org.apache.iceberg:iceberg-api:${libs.versions.iceberg.get()}:tests")
  testImplementation("org.apache.iceberg:iceberg-core:${libs.versions.iceberg.get()}:tests")

  testImplementation("org.apache.iceberg:iceberg-spark-3.5_2.12")
  testImplementation("org.apache.iceberg:iceberg-spark-extensions-3.5_2.12")
  testImplementation(libs.spark35.sql.scala212) {
    // exclude log4j dependencies
    exclude("org.apache.logging.log4j", "log4j-slf4j2-impl")
    exclude("org.apache.logging.log4j", "log4j-api")
    exclude("org.apache.logging.log4j", "log4j-1.2-api")
    exclude("org.slf4j", "jul-to-slf4j")
  }

  testImplementation("software.amazon.awssdk:glue")
  testImplementation("software.amazon.awssdk:kms")
  testImplementation("software.amazon.awssdk:dynamodb")

  testImplementation(platform(libs.quarkus.bom))
  testImplementation("io.quarkus:quarkus-junit5")
  testImplementation("io.quarkus:quarkus-junit5-mockito")
  testImplementation("io.quarkus:quarkus-rest-client")
  testImplementation("io.quarkus:quarkus-rest-client-jackson")
  testImplementation("io.rest-assured:rest-assured")

  testImplementation(platform(libs.testcontainers.bom))
  testImplementation("org.testcontainers:testcontainers")
  testImplementation(libs.s3mock.testcontainers)

  testImplementation(libs.hawkular.agent.prometheus.scraper)

  intTestImplementation(project(":polaris-api-management-model"))

  intTestImplementation("org.apache.iceberg:iceberg-api:${libs.versions.iceberg.get()}")
  intTestImplementation("org.apache.iceberg:iceberg-core:${libs.versions.iceberg.get()}")
  intTestImplementation("org.apache.iceberg:iceberg-api:${libs.versions.iceberg.get()}:tests")
  intTestImplementation("org.apache.iceberg:iceberg-core:${libs.versions.iceberg.get()}:tests")

  intTestImplementation(platform(libs.quarkus.bom))
  intTestImplementation("io.quarkus:quarkus-junit5")

  // required for QuarkusSparkIT
  intTestImplementation(enforcedPlatform(libs.scala212.lang.library))
  intTestImplementation(enforcedPlatform(libs.scala212.lang.reflect))
  intTestImplementation(libs.javax.servlet.api)
  intTestImplementation(libs.antlr4.runtime)
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

  val logsDir = project.layout.buildDirectory.get().asFile.resolve("logs")

  // JVM arguments provider does not interfere with Gradle's cache keys
  jvmArgumentProviders.add(
    CommandLineArgumentProvider {
      // Same issue as above: allow a java security manager after Java 21
      // (this setting is for the application under test, while the setting above is for test code).
      val securityManagerAllow = "-Djava.security.manager=allow"

      val args = mutableListOf<String>()

      // Do not run the integration tests against the docker image
      args.add("-Dquarkus.test.integration-test-artifact-type=jar")

      // Example: to attach a debugger to the spawned JVM running Quarkus, add
      // -Dquarkus.test.arg-line=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005
      // to your test configuration.
      val explicitQuarkusTestArgLine = System.getProperty("quarkus.test.arg-line")
      val quarkusTestArgLine =
        if (explicitQuarkusTestArgLine != null) "$explicitQuarkusTestArgLine $securityManagerAllow"
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

quarkus {
  quarkusBuildProperties.put("quarkus.package.jar.type", "fast-jar")
  // Pull manifest attributes from the "main" `jar` task to get the
  // release-information into the jars generated by Quarkus.
  quarkusBuildProperties.putAll(
    provider {
      tasks
        .named("jar", Jar::class.java)
        .get()
        .manifest
        .attributes
        .map { e -> "quarkus.package.jar.manifest.attributes.\"${e.key}\"" to e.value.toString() }
        .toMap()
    }
  )
}

tasks.named("distZip") { dependsOn("quarkusBuild") }

tasks.named("distTar") { dependsOn("quarkusBuild") }

tasks.register("run") { dependsOn("quarkusRun") }

tasks.named<QuarkusRun>("quarkusRun") {
  jvmArgs =
    listOf("-Dpolaris.bootstrap.credentials=POLARIS,root,secret", "-Dquarkus.console.color=true")
}

distributions {
  main {
    contents {
      from(project.layout.buildDirectory.dir("quarkus-app"))
      from("../../NOTICE")
      from("../../LICENSE-BINARY-DIST").rename("LICENSE-BINARY-DIST", "LICENSE")
      exclude("lib/main/io.quarkus.quarkus-container-image*")
    }
  }
}

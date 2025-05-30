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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  alias(libs.plugins.openapi.generator)
  id("polaris-server")
  // id("polaris-license-report")
  id("polaris-shadow-jar")
  id("application")
}

dependencies {
  implementation(project(":polaris-core"))
  implementation(project(":polaris-api-management-service"))
  implementation(project(":polaris-api-iceberg-service"))
  implementation(project(":polaris-service-common"))

  implementation(platform(libs.dropwizard.bom))
  implementation("io.dropwizard:dropwizard-core")
  implementation("io.dropwizard:dropwizard-auth")
  implementation("io.dropwizard:dropwizard-json-logging")

  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")
  implementation("org.apache.iceberg:iceberg-aws")

  // override dnsjava version in dependencies due to https://github.com/dnsjava/dnsjava/issues/329
  implementation(platform(libs.dnsjava))

  implementation(libs.hadoop.common) {
    exclude("org.slf4j", "slf4j-reload4j")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("ch.qos.reload4j", "reload4j")
    exclude("log4j", "log4j")
    exclude("org.apache.zookeeper", "zookeeper")
    exclude("org.apache.hadoop.thirdparty", "hadoop-shaded-protobuf_3_25")
    exclude("com.github.pjfanning", "jersey-json")
    exclude("com.sun.jersey", "jersey-core")
    exclude("com.sun.jersey", "jersey-server")
    exclude("com.sun.jersey", "jersey-servlet")
  }

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.servlet.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.ws.rs.api)

  compileOnly(libs.smallrye.common.annotation)

  implementation(platform(libs.google.cloud.storage.bom))
  implementation("com.google.cloud:google-cloud-storage")

  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:sts")
  implementation("software.amazon.awssdk:iam-policy-builder")
  implementation("software.amazon.awssdk:s3")

  implementation(platform(libs.azuresdk.bom))
  implementation("com.azure:azure-core")

  implementation(platform(libs.micrometer.bom))
  implementation("io.micrometer:micrometer-core")
  implementation("io.micrometer:micrometer-registry-prometheus")
  implementation(libs.prometheus.metrics.exporter.servlet.jakarta)

  implementation(platform(libs.opentelemetry.bom))
  implementation("io.opentelemetry:opentelemetry-api")
  implementation("io.opentelemetry:opentelemetry-sdk-trace")
  implementation("io.opentelemetry:opentelemetry-exporter-logging")
  implementation(libs.opentelemetry.semconv)

  implementation(libs.smallrye.common.annotation)

  compileOnly(libs.swagger.annotations)
  compileOnly(libs.spotbugs.annotations)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.spotbugs.annotations)

  testImplementation(project(":polaris-api-management-model"))

  testImplementation("org.apache.iceberg:iceberg-api:${libs.versions.iceberg.get()}:tests")
  testImplementation("org.apache.iceberg:iceberg-core:${libs.versions.iceberg.get()}:tests")
  testImplementation("io.dropwizard:dropwizard-testing")
  testImplementation(platform(libs.testcontainers.bom))
  testImplementation("org.testcontainers:testcontainers")
  testImplementation(libs.s3mock.testcontainers)

  testImplementation("org.apache.iceberg:iceberg-spark-3.5_2.12")
  testImplementation("org.apache.iceberg:iceberg-spark-extensions-3.5_2.12")
  testImplementation("org.apache.spark:spark-sql_2.12:3.5.6") {
    // exclude log4j dependencies
    exclude("org.apache.logging.log4j", "log4j-slf4j2-impl")
    exclude("org.apache.logging.log4j", "log4j-api")
    exclude("org.apache.logging.log4j", "log4j-1.2-api")
  }

  testImplementation(platform(libs.awssdk.bom))
  testImplementation("software.amazon.awssdk:glue")
  testImplementation("software.amazon.awssdk:kms")
  testImplementation("software.amazon.awssdk:dynamodb")

  testImplementation(libs.auth0.jwt)

  testCompileOnly(libs.smallrye.common.annotation)

  testImplementation(platform(libs.junit.bom))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation(libs.assertj.core)
  testImplementation(libs.mockito.core)
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")

  testImplementation(project(":polaris-eclipselink"))
}

if (project.properties.get("eclipseLink") == "true") {
  dependencies { implementation(project(":polaris-eclipselink")) }
}

tasks.named<Test>("test").configure {
  if (System.getenv("AWS_REGION") == null) {
    environment("AWS_REGION", "us-west-2")
  }
  jvmArgs("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED")
  useJUnitPlatform()
  maxParallelForks = 4
}

tasks.register<JavaExec>("runApp").configure {
  if (System.getenv("AWS_REGION") == null) {
    environment("AWS_REGION", "us-west-2")
  }
  classpath = sourceSets["main"].runtimeClasspath
  mainClass = "org.apache.polaris.service.dropwizard.PolarisApplication"
  args("server", "$rootDir/polaris-server.yml")
}

application { mainClass = "org.apache.polaris.service.dropwizard.PolarisApplication" }

tasks.named<Jar>("jar") {
  manifest { attributes["Main-Class"] = "org.apache.polaris.service.dropwizard.PolarisApplication" }
}

tasks.register<Jar>("testJar") {
  archiveClassifier.set("tests")
  from(sourceSets.test.get().output)
}

val shadowJar =
  tasks.named<ShadowJar>("shadowJar") {
    append("META-INF/hk2-locator/default")
    finalizedBy("startShadowScripts")
  }

val startScripts =
  tasks.named<CreateStartScripts>("startScripts") { applicationName = "polaris-service" }

tasks.named<CreateStartScripts>("startShadowScripts") { applicationName = "polaris-service" }

tasks.register<Sync>("prepareDockerDist") {
  into(project.layout.buildDirectory.dir("docker-dist"))
  from(startScripts) { into("bin") }
  from(configurations.runtimeClasspath) { into("lib") }
  from(tasks.named<Jar>("jar")) { into("lib") }
}

tasks.named("build").configure { dependsOn("prepareDockerDist") }

distributions {
  main {
    contents {
      from("../../NOTICE")
      from("../../LICENSE-BINARY-DIST").rename("LICENSE-BINARY-DIST", "LICENSE")
    }
  }
  named("shadow") {
    contents {
      from("../../NOTICE")
      from("../../LICENSE-BINARY-DIST").rename("LICENSE-BINARY-DIST", "LICENSE")
    }
  }
}

tasks.named("assemble").configure { dependsOn("testJar") }

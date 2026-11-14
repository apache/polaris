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

import java.time.Duration
import org.gradle.api.component.AdhocComponentWithVariants

plugins {
  id("org.kordamp.gradle.jandex")
  id("polaris-server")
  id("polaris-server-test-runner")
}

val quarkusRuntimeOnly =
  configurations.dependencyScope("quarkusRuntimeOnly") {
    extendsFrom(configurations.implementation.get(), configurations.runtimeOnly.get())
    // spotbugs-annotations has only a GPL license.
    exclude("com.github.spotbugs", "spotbugs-annotations")
  }

configurations.named("testImplementation") { extendsFrom(quarkusRuntimeOnly.get()) }

val quarkusRuntimeElements =
  configurations.consumable("quarkusRuntimeElements") {
    extendsFrom(quarkusRuntimeOnly.get())
    attributes {
      addAllLater(configurations.runtimeElements.get().attributes)
    }
    outgoing {
      artifact(tasks.named("jar"))
      capability("$group:${project.name}-quarkus:$version")
    }
  }

(components["java"] as AdhocComponentWithVariants).addVariantsFromConfiguration(
  quarkusRuntimeElements.get(),
) {}

val cassandraStartupAction = sourceSets.create("cassandraStartupAction")
val cassandraStartupActionCompileOnly =
  configurations.getByName(cassandraStartupAction.compileOnlyConfigurationName)
val cassandraStartupActionImplementation =
  configurations.getByName(cassandraStartupAction.implementationConfigurationName)

dependencies {
  polarisServer(project(path = ":polaris-server", configuration = "quarkusRunner"))
  cassandraStartupActionCompileOnly(
    "org.apache.polaris.server-test-runner:polaris-server-test-runner",
  )
  cassandraStartupActionImplementation(testFixtures(project()))
  cassandraStartupActionImplementation(project(":polaris-container-spec-helper"))
  cassandraStartupActionImplementation(platform(libs.testcontainers.bom))
  cassandraStartupActionImplementation("org.testcontainers:testcontainers-cassandra") {
    exclude("com.datastax.cassandra", "cassandra-driver-core")
  }

  implementation(project(":polaris-persistence-nosql-api"))
  implementation(project(":polaris-persistence-nosql-impl"))
  implementation(project(":polaris-idgen-api"))
  compileOnly(project(":polaris-persistence-nosql-cdi-quarkus"))

  implementation(platform(libs.cassandra.driver.bom))
  implementation("org.apache.cassandra:java-driver-core") {
    // spotbugs-annotations has only a GPL license!
    exclude("com.github.spotbugs", "spotbugs-annotations")
  }

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  compileOnly(libs.jspecify)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  implementation(libs.smallrye.config.core)
  compileOnly(platform(libs.quarkus.bom))
  compileOnly("io.quarkus:quarkus-core")
  compileOnly("io.quarkus:quarkus-tls-registry")
  compileOnly("io.quarkus:quarkus-smallrye-health")
  compileOnly("io.quarkus:quarkus-micrometer")
  compileOnly("org.apache.cassandra:java-driver-metrics-micrometer")
  add("quarkusRuntimeOnly", platform(libs.quarkus.bom))
  add("quarkusRuntimeOnly", "io.quarkus:quarkus-tls-registry")
  add("quarkusRuntimeOnly", "io.quarkus:quarkus-smallrye-health")
  add("quarkusRuntimeOnly", "io.quarkus:quarkus-micrometer")
  add("quarkusRuntimeOnly", "org.apache.cassandra:java-driver-metrics-micrometer")

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.fasterxml.jackson.core:jackson-databind")

  testFixturesApi(testFixtures(project(":polaris-persistence-nosql-impl")))
  testFixturesApi(project(":polaris-persistence-nosql-testextension"))

  testFixturesCompileOnly(libs.jakarta.annotation.api)
  testFixturesCompileOnly(libs.jakarta.validation.api)

  testFixturesCompileOnly(project(":polaris-immutables"))
  testFixturesAnnotationProcessor(project(":polaris-immutables", configuration = "processor"))

  testFixturesImplementation(project(":polaris-container-spec-helper"))

  testFixturesImplementation(platform(libs.cassandra.driver.bom))
  testFixturesImplementation("org.apache.cassandra:java-driver-core")

  testFixturesImplementation(platform(libs.testcontainers.bom))
  testFixturesImplementation("org.testcontainers:testcontainers-cassandra") {
    exclude("com.datastax.cassandra", "cassandra-driver-core")
  }

  testImplementation(libs.smallrye.config.core)
  testImplementation(libs.jakarta.enterprise.cdi.api)
}

testing {
  suites {
    register<JvmTestSuite>("serverIntTest") {
      dependencies {
        implementation(platform(libs.quarkus.bom))
        implementation("io.rest-assured:rest-assured")
        runtimeOnly("org.slf4j:jcl-over-slf4j:${libs.slf4j.api.get().version}")
      }
      targets {
        all {
          val buildDir = project.layout.buildDirectory
          testTask.configure {
            withPolarisServer(configurations.polarisServer) {
              startupTimeout.set(Duration.ofMinutes(2))
              startupActionClasspath.from(cassandraStartupAction.runtimeClasspath)
              startupActionClass.set(
                "org.apache.polaris.persistence.nosql.cassandra.CassandraStartupAction",
              )
              environment.put("POLARIS_BOOTSTRAP_CREDENTIALS", "POLARIS,test-admin,test-secret")
              systemProperties.put(
                "quarkus.log.file.path",
                buildDir.get().asFile.resolve("logs/serverIntTest/polaris.log").absolutePath,
              )
              systemProperties.put("polaris.readiness.ignore-severe-issues", "true")
            }
          }
        }
      }
    }
    register<JvmTestSuite>("intTest") {
      dependencies {
        compileOnly(platform(libs.jackson3.bom))
        compileOnly("com.fasterxml.jackson.core:jackson-annotations")
      }
    }
  }
}

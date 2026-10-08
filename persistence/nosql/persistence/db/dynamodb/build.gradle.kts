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

import org.gradle.api.component.AdhocComponentWithVariants

plugins {
  id("org.kordamp.gradle.jandex")
  id("polaris-server")
  id("polaris-server-test-runner")
}

val quarkusRuntimeOnly =
  configurations.dependencyScope("quarkusRuntimeOnly") {
    extendsFrom(configurations.implementation.get(), configurations.runtimeOnly.get())
  }
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
  quarkusRuntimeElements.get()
) {}

val dynamoDbStartupAction = sourceSets.create("dynamoDbStartupAction")
val dynamoDbStartupActionCompileOnly =
  configurations.getByName(dynamoDbStartupAction.compileOnlyConfigurationName)
val dynamoDbStartupActionImplementation =
  configurations.getByName(dynamoDbStartupAction.implementationConfigurationName)

dependencies {
  polarisServer(project(path = ":polaris-server", configuration = "quarkusRunner"))
  dynamoDbStartupActionCompileOnly(
    "org.apache.polaris.server-test-runner:polaris-server-test-runner"
  )
  dynamoDbStartupActionImplementation(project(":polaris-floci-aws-testcontainer"))

  implementation(project(":polaris-persistence-nosql-api"))
  implementation(project(":polaris-persistence-nosql-impl"))
  implementation(project(":polaris-idgen-api"))
  compileOnly(project(":polaris-persistence-nosql-cdi-quarkus"))

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:dynamodb")
  implementation("software.amazon.awssdk:apache5-client")

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.fasterxml.jackson.core:jackson-databind")

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.smallrye.config.core)
  compileOnly(platform(libs.quarkus.bom))
  compileOnly(platform(libs.quarkus.amazon.services.bom))
  compileOnly("io.quarkus:quarkus-core")
  compileOnly("io.quarkiverse.amazonservices:quarkus-amazon-dynamodb")
  add("quarkusRuntimeOnly", platform(libs.quarkus.bom))
  add("quarkusRuntimeOnly", platform(libs.quarkus.amazon.services.bom))
  add("quarkusRuntimeOnly", "io.quarkiverse.amazonservices:quarkus-amazon-dynamodb")
  add("quarkusRuntimeOnly", "software.amazon.awssdk:url-connection-client")

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testFixturesApi(testFixtures(project(":polaris-persistence-nosql-impl")))
  testFixturesApi(project(":polaris-persistence-nosql-testextension"))

  testFixturesCompileOnly(libs.jakarta.annotation.api)
  testFixturesCompileOnly(libs.jakarta.validation.api)

  testFixturesCompileOnly(project(":polaris-immutables"))
  testFixturesAnnotationProcessor(project(":polaris-immutables", configuration = "processor"))

  testFixturesImplementation(project(":polaris-floci-aws-testcontainer"))

  testFixturesImplementation(platform(libs.awssdk.bom))
  testFixturesImplementation("software.amazon.awssdk:dynamodb")
  testFixturesImplementation("software.amazon.awssdk:apache-client")
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
              startupActionClasspath.from(dynamoDbStartupAction.runtimeClasspath)
              startupActionClass.set(
                "org.apache.polaris.persistence.nosql.dynamodb.DynamoDbStartupAction"
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
        runtimeOnly(platform(libs.testcontainers.bom))
        runtimeOnly("org.testcontainers:testcontainers")
        runtimeOnly(libs.docker.java.api)
      }
    }
  }
}

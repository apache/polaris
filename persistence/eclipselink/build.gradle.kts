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

fun isValidDep(dep: String): Boolean {
  val depRegex = "^[\\w.]+:[\\w\\-.]+:[\\w\\-.]+$".toRegex()
  return dep.matches(depRegex)
}

plugins {
  id("polaris-server")
  alias(libs.plugins.quarkus)
  alias(libs.plugins.jandex)
}

dependencies {
  implementation(project(":polaris-core"))

  implementation(libs.eclipselink)

  implementation(libs.slf4j.api)

  val eclipseLinkDeps: String? = project.findProperty("eclipseLinkDeps") as String?
  eclipseLinkDeps?.let {
    val dependenciesList = it.split(",")
    dependenciesList.forEach { dep ->
      val trimmedDep = dep.trim()
      if (isValidDep(trimmedDep)) {
        implementation(trimmedDep)
      } else {
        throw GradleException("Invalid dependency format: $trimmedDep")
      }
    }
  }

  // only for @VisibleForTesting
  compileOnly(libs.guava)

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.smallrye.common.annotation) // @Identifier
  compileOnly(libs.smallrye.config.core) // @ConfigMapping

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.fasterxml.jackson.core:jackson-core")

  testImplementation(libs.h2)
  testImplementation(testFixtures(project(":polaris-core")))
}

tasks.register<Jar>("createTestConfJar") {
  archiveFileName = "test-conf.jar"
  destinationDirectory = layout.buildDirectory.dir("conf/eclipselink")
  from("src/main/resources/META-INF/") { include("persistence.xml") }
}

sourceSets { test { resources.srcDir(layout.buildDirectory.dir("conf")) } }

tasks.named("processTestResources") { dependsOn("createTestConfJar") }

tasks.named("javadoc") { dependsOn("jandex") }

tasks.named("quarkusDependenciesBuild") { dependsOn("jandex") }

tasks.named("compileJava") { dependsOn("compileQuarkusGeneratedSourcesJava") }

tasks.named("sourcesJar") { dependsOn("compileQuarkusGeneratedSourcesJava") }

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
  `java-library`
}

dependencies {
  implementation(project(":polaris-core"))
  implementation(project(":polaris-jpa-model"))
  implementation(libs.eclipselink)
  implementation(platform(libs.dropwizard.bom))
  implementation(libs.jakarta.inject.api)
  implementation(libs.smallrye.common.annotation)
  implementation("io.dropwizard:dropwizard-jackson")
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

  compileOnly(libs.jakarta.annotation.api)

  testImplementation(libs.h2)
  testImplementation(testFixtures(project(":polaris-core")))

  testImplementation(platform(libs.junit.bom))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation(libs.assertj.core)
  testImplementation(libs.mockito.core)
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.register<Jar>("createTestConfJar") {
  archiveFileName = "test-conf.jar"
  destinationDirectory = layout.buildDirectory.dir("conf/eclipselink")
  from("src/main/resources/META-INF/") { include("persistence.xml") }
}

sourceSets { test { resources.srcDir(layout.buildDirectory.dir("conf")) } }

tasks.named("processTestResources") { dependsOn("createTestConfJar") }

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

import org.apache.tools.ant.filters.ReplaceTokens

plugins {
  id("polaris-apprunner-java")
  alias(libs.plugins.maven.plugin)
}

val deps by configurations.creating
val maven by configurations.creating

configurations.implementation.get().extendsFrom(deps)

dependencies {
  deps(project(":polaris-apprunner-common"))
  implementation(libs.maven.core)
  compileOnly(libs.maven.plugin.annotations)
  compileOnly(libs.jakarta.annotation.api)
  testImplementation(libs.soebes.itf.jupiter.extension)
  testImplementation(libs.soebes.itf.assertj)
  maven(
    group = "org.apache.maven",
    name = "apache-maven",
    version = libs.maven.core.get().version,
    classifier = "bin",
    ext = "tar.gz",
  )
}

mavenPlugin {
  helpMojoPackage.set("org.apache.polaris.apprunner.maven")
  artifactId = project.name
  groupId = project.group.toString()
  dependencies = deps
}

// The following stuff is needed by the Maven integration tests.
//
// The Maven plugin integration tests use `com.soebes.itf.jupiter.extension`, which is admittedly a
// bit "dusty",
// but it works. That extension however is built to be run inside a Maven build, but here it is
// Gradle, so we
// have to do some things manually to get that Maven-plugin-integration-test-framework working.

// Does what it says, download and unpack a Maven distribution, needed by the IT-framework.
val getMavenDistro by
  tasks.registering(Sync::class) {
    from(tarTree(maven.singleFile)) { eachFile { path = path.substring(path.indexOf('/') + 1) } }
    into(layout.buildDirectory.dir("maven"))
  }

// soebes-itf expects the artifacts of/for the Maven plugin to be tested in `target/itf-repo` in the
// layout of
// a local Maven repo. Sadly, Gradle offers no standard way to publish artifacts to a "custom" local
// Maven repo,
// so this task publishes the required artifacts to the user's local Maven repo and then copies the
// published
// artifacts to `target/itf-repo`.
val itfRepo by
  tasks.registering(Sync::class) {
    // polaris-apprunner parent pom
    dependsOn(":publishToMavenLocal")
    // polaris-apprunner-common pom + jar
    dependsOn(":polaris-apprunner-common:publishToMavenLocal")
    // polaris-apprunner-maven-plugin pom + jar
    dependsOn("publishToMavenLocal")

    // Poor-man's way to convert the group-ID to a path
    val groupPath = project.group.toString().replace(".", "/")
    // Note: this assumes the user has his local Maven repository in $HOME/.m2/repository. This does
    // NOT work for
    // any other location, whether it's configured using Maven properties or a settings.xml. If such
    // a support is
    // required, please add support for that and open a PR.
    val localMavenRepo = "${System.getProperty("user.home")}/.m2/repository"
    from(localMavenRepo)
    include("$groupPath/**")
    into(layout.projectDirectory.dir("target/itf-repo"))
  }

// Copy the Maven projects used by the integration-tests, while replacing the necessary placeholders
// for GAV and
// dependencies used by those tests.
val syncResourcesIts by
  tasks.registering(Sync::class) {
    from("src/test/resources-its")
    into(project.layout.projectDirectory.dir("target/test-classes"))
    filter(
      ReplaceTokens::class,
      mapOf(
        "tokens" to
          mapOf(
            "projectGroupId" to project.group.toString(),
            "projectArtifactId" to project.name,
            "projectVersion" to project.version,
            "junitVersion" to libs.junit.bom.get().version,
          )
      ),
    )
  }

tasks.named<Test>("test") {
  dependsOn(syncResourcesIts, itfRepo, getMavenDistro)
  jvmArgumentProviders.add(
    CommandLineArgumentProvider {
      listOf("-Dmaven.home=${getMavenDistro.get().outputs.files.singleFile}")
    }
  )
  environment(
    mapOf("JAVA_HOME" to this.javaLauncher.get().metadata.installationPath.asFile.toString())
  )
}

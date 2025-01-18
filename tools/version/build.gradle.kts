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

plugins { id("polaris-client") }

dependencies {
  compileOnly(libs.picocli)
  testFixturesApi(libs.assertj.core)
}

description =
  "Provides Polaris version information programmatically, includes the NOTICE/LICENSE* files"

val syncNoticeAndLicense by
  tasks.registering(Sync::class) {
    // Have to manually declare the inputs of this task here on top of the from/include below
    inputs.files(rootProject.layout.files("NOTICE*", "LICENSE*", "version.txt"))
    inputs.property("version", project.version)
    destinationDir = project.layout.buildDirectory.dir("notice-licenses").get().asFile
    from(rootProject.rootDir) {
      include("NOTICE*", "LICENSE*")
      // Put NOTICE/LICENSE* files under META-INF/resources/ so those files can be directly
      // accessed as static web resources in Quarkus.
      eachFile { path = "META-INF/resources/apache-polaris/${file.name}.txt" }
    }
    from(rootProject.rootDir) {
      include("version.txt")
      // Put NOTICE/LICENSE* files under META-INF/resources/ so those files can be directly
      // accessed as static web resources in Quarkus.
      eachFile { path = "META-INF/resources/apache-polaris/${file.name}" }
    }
  }

val versionProperties by
  tasks.registering(Sync::class) {
    destinationDir = project.layout.buildDirectory.dir("version").get().asFile
    from(project.layout.files("src/main/version"))
    eachFile { path = "org/apache/polaris/version/$path" }
    inputs.property("projectVersion", project.version)
    filter(ReplaceTokens::class, mapOf("tokens" to mapOf("projectVersion" to project.version)))
  }

sourceSets.main.configure {
  resources {
    srcDir(syncNoticeAndLicense)
    srcDir(versionProperties)
  }
}

// Build a jar for `jarTest` having both the production and test sources including the "fake
// manifest" - the production implementation expects all resources to be in the jar containing
// the `polaris-version.properties` file.
val jarTestJar by
  tasks.registering(Jar::class) {
    archiveClassifier.set("jarTest")
    from(sourceSets.main.get().output)
    from(sourceSets.getByName("jarTest").output)
  }

// Add a test-suite to run against the built polaris-version*.jar, not the classes/, because we
// need to test the `jar:` scheme/protocol resolution.
testing {
  suites {
    register<JvmTestSuite>("jarTest") {
      dependencies { runtimeOnly(files(jarTestJar.get().archiveFile.get().asFile)) }

      targets.all {
        testTask.configure {
          dependsOn("jar", jarTestJar)
          systemProperty("rootProjectDir", rootProject.rootDir.relativeTo(project.projectDir))
          systemProperty("polarisVersion", project.version)
        }
      }
    }
  }
}

tasks.named("test") { dependsOn("jarTest") }

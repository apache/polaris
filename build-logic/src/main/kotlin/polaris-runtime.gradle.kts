/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.gradle.api.plugins.jvm.JvmTestSuite
import org.gradle.kotlin.dsl.register
import org.gradle.kotlin.dsl.withType

plugins { id("polaris-server") }

testing {
  suites {
    withType<JvmTestSuite> {
      targets.all {
        if (testTask.name != "test") {
          testTask.configure {
            // For Quarkus...
            //
            // io.quarkus.test.junit.IntegrationTestUtil.determineBuildOutputDirectory(java.net.URL)
            // is not smart enough :(
            systemProperty("build.output.directory", layout.buildDirectory.asFile.get())
            dependsOn(tasks.named("quarkusBuild"))
          }
        }
      }
    }
    register<JvmTestSuite>("intTest") {
      targets.all {
        tasks.named("compileIntTestJava").configure {
          dependsOn(tasks.named("compileQuarkusTestGeneratedSourcesJava"))
        }
      }
      sources { java.srcDirs(tasks.named("quarkusGenerateCodeTests")) }
    }
  }
}

// Let the test's implementation config extend testImplementation, so it also inherits the
// project's "main" implementation dependencies (not just the "api" configuration)
configurations.named("intTestImplementation").configure {
  extendsFrom(configurations.getByName("testImplementation"))
}

dependencies { add("intTestImplementation", java.sourceSets.getByName("test").output.dirs) }

configurations.named("intTestRuntimeOnly").configure {
  extendsFrom(configurations.getByName("testRuntimeOnly"))
}

tasks.named("compileJava") { dependsOn("compileQuarkusGeneratedSourcesJava") }

tasks.named("sourcesJar") { dependsOn("compileQuarkusGeneratedSourcesJava") }

tasks.named("javadoc") { dependsOn("jandex") }

tasks.named("quarkusDependenciesBuild") { dependsOn("jandex") }

tasks.named("imageBuild") { dependsOn("jandex") }

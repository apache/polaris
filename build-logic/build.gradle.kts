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

plugins { `kotlin-dsl` }

dependencies {
  implementation(gradleKotlinDsl())
  implementation(baselibs.errorprone)
  implementation(baselibs.idea.ext)
  implementation(baselibs.jandex)
  implementation(baselibs.nexus.publish)
  implementation(baselibs.shadow)
  implementation(baselibs.spotless)
  testImplementation(gradleTestKit())
  testImplementation(platform(libs.junit.bom))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation(libs.assertj.core)
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

testing {
  suites {
    named<JvmTestSuite>("test") { useJUnitJupiter() }

    // Gradle TestKit tests of the changelog tasks run real Gradle builds, so they are much slower
    // than the unit tests and live in a separate suite, run with
    // `./gradlew :polaris-build-logic:integrationTest`.
    register<JvmTestSuite>("integrationTest") {
      useJUnitJupiter()
      dependencies {
        implementation(project())
        implementation(gradleTestKit())
        implementation(platform(libs.junit.bom))
        implementation("org.junit.jupiter:junit-jupiter")
        implementation(libs.assertj.core)
        runtimeOnly("org.junit.platform:junit-platform-launcher")
      }
      targets.configureEach {
        testTask.configure {
          // The fixture projects use the task classes of this project
          dependsOn(tasks.jar)
          systemProperty("build-logic.jar", tasks.jar.get().archiveFile.get().asFile.absolutePath)
        }
      }
    }
  }
}

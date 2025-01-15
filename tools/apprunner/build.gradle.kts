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

import java.net.URI

buildscript { repositories { maven { url = java.net.URI("https://plugins.gradle.org/m2/") } } }

plugins { id("polaris-apprunner-root") }

version = rootProject.rootDir.resolve("../../version.txt").readText().trim()

publishingHelper {
  asfProjectName = "polaris"
  overrideName = "polaris-apprunner"
  overrideDescription = "Polaris Apprunner Gradle + Maven plugins"
}

// Pass environment variables:
//    ORG_GRADLE_PROJECT_apacheUsername
//    ORG_GRADLE_PROJECT_apachePassword
// OR in ~/.gradle/gradle.properties set
//    apacheUsername
//    apachePassword
// Call targets:
//    publishToApache
//    closeApacheStagingRepository
//    releaseApacheStagingRepository
//       or closeAndReleaseApacheStagingRepository
//
// Username is your ASF ID
// Password: your ASF LDAP password - or better: a token generated via
// https://repository.apache.org/
nexusPublishing {
  transitionCheckOptions {
    // default==60 (10 minutes), wait up to 120 minutes
    maxRetries = 720
    // default 10s
    //        delayBetween = java.time.Duration.ofSeconds(10)
  }

  repositories {
    register("apache") {
      nexusUrl = URI.create("https://repository.apache.org/service/local/")
      snapshotRepositoryUrl =
        URI.create("https://repository.apache.org/content/repositories/snapshots/")
    }
  }
}

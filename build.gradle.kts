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
import org.nosphere.apache.rat.RatTask

buildscript {
  repositories { maven { url = java.net.URI("https://plugins.gradle.org/m2/") } }
  dependencies {
    classpath("com.diffplug.spotless:spotless-plugin-gradle:${libs.plugins.spotless.get().version}")
  }
}

plugins {
  id("idea")
  id("eclipse")
  id("polaris-root")
  alias(libs.plugins.rat)
}

val projectName = rootProject.file("ide-name.txt").readText().trim()
val ideName = "$projectName ${rootProject.version.toString().replace("^([0-9.]+).*", "\\1")}"

if (System.getProperty("idea.sync.active").toBoolean()) {
  // There's no proper way to set the name of the IDEA project (when "just importing" or
  // syncing the Gradle project)
  val ideaDir = rootProject.layout.projectDirectory.dir(".idea")
  ideaDir.asFile.mkdirs()
  ideaDir.file(".name").asFile.writeText(ideName)

  val icon = ideaDir.file("icon.png").asFile
  if (!icon.exists()) {
    copy {
      from("docs/img/logos/polaris-brandmark.png")
      into(ideaDir)
      rename { _ -> "icon.png" }
    }
  }
}

eclipse { project { name = ideName } }

tasks.named<RatTask>("rat").configure {
  // These are Gradle file pattern syntax
  excludes.add("**/build/**")

  excludes.add("docs/CNAME")
  excludes.add("docs/index.html")

  excludes.add("DISCLAIMER_WIP")
  excludes.add("LICENSE")
  excludes.add("NOTICE")

  excludes.add("ide-name.txt")
  excludes.add("version.txt")
  excludes.add(".git")
  excludes.add(".gradle")
  excludes.add(".idea")
  excludes.add(".java-version")
  excludes.add("**/.keep")
  excludes.add("**/poetry.lock")

  excludes.add(".github/pull_request_template.md")

  excludes.add("spec/docs.yaml")
  excludes.add("spec/index.yml")

  excludes.add("gradle/wrapper/gradle-wrapper*.jar*")

  excludes.add("logs/**")
  excludes.add("polaris-service/src/**/banner.txt")
  excludes.add("polaris-service/logs")

  excludes.add("site/node_modules/**")
  excludes.add("site/layouts/robots.txt")
  // Ignore generated stuff, when the Hugo is run w/o Docker
  excludes.add("site/public/**")
  excludes.add("site/resources/_gen/**")
  excludes.add("node_modules/**")

  excludes.add("**/polaris-venv/**")

  excludes.add("**/.pytest_cache/**")
  excludes.add("regtests/**/py.typed")
  excludes.add("regtests/**/*.ref")
  excludes.add("regtests/.env")
  excludes.add("regtests/derby.log")
  excludes.add("regtests/metastore_db/**")
  excludes.add("regtests/client/python/.openapi-generator/**")
  excludes.add("regtests/output/**")

  excludes.add("**/*.ipynb")
  excludes.add("**/*.iml")
  excludes.add("**/*.iws")

  excludes.add("**/*.md")

  excludes.add("**/*.png")
  excludes.add("**/*.svg")

  excludes.add("**/*.lock")

  excludes.add("**/*.env*")

  excludes.add("**/go.sum")

  excludes.add("**/kotlin-compiler*")
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
    delayBetween = java.time.Duration.ofSeconds(10)
  }

  repositories {
    register("apache") {
      nexusUrl = URI.create("https://repository.apache.org/service/local/")
      snapshotRepositoryUrl =
        URI.create("https://repository.apache.org/content/repositories/snapshots/")
    }
  }
}

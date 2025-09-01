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

buildscript { repositories { maven { url = java.net.URI("https://plugins.gradle.org/m2/") } } }

plugins {
  id("idea")
  id("eclipse")
  id("polaris-root")
  alias(libs.plugins.rat)
  alias(libs.plugins.jetbrains.changelog)
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
      from("site/static/img/logos/polaris-brandmark.png")
      into(ideaDir)
      rename { _ -> "icon.png" }
    }
  }
}

eclipse { project { name = ideName } }

tasks.named<RatTask>("rat").configure {
  // Gradle
  excludes.add("**/build/**")
  excludes.add("gradle/wrapper/gradle-wrapper*")
  excludes.add(".gradle")
  excludes.add("**/kotlin-compiler*")
  excludes.add("**/build-logic/.kotlin/**")

  excludes.add("ide-name.txt")
  excludes.add("version.txt")

  excludes.add("DISCLAIMER_WIP")
  excludes.add("LICENSE")
  excludes.add("NOTICE")

  // Files copied from Docsy (ASLv2 licensed) don't have header
  excludes.add("site/layouts/docs/baseof.html")
  excludes.add("site/layouts/shortcodes/redoc-polaris.html")
  excludes.add("site/layouts/community/list.html")
  excludes.add("site/layouts/partials/navbar.html")
  excludes.add("site/layouts/partials/head.html")
  excludes.add("site/layouts/partials/community_links.html")
  excludes.add("layouts/partials/head.html")

  // Files copied from OpenAPI Generator (ASLv2 licensed) don't have header
  excludes.add("server-templates/*.mustache")

  // Manifest files do not allow comments
  excludes.add("tools/version/src/jarTest/resources/META-INF/FAKE_MANIFEST.MF")

  // Git & GitHub
  excludes.add(".git")
  excludes.add(".github/pull_request_template.md")

  // Misc build artifacts
  excludes.add(".java-version")
  excludes.add("**/.keep")
  excludes.add("logs/**")
  excludes.add("**/*.lock")

  // Polaris service startup banner
  excludes.add("runtime/service/src/**/banner.txt")

  // Web site
  excludes.add("**/go.sum")
  excludes.add("site/.user-settings")
  excludes.add("site/node_modules/**")
  excludes.add("site/layouts/robots.txt")
  // Ignore generated stuff, when the Hugo is run w/o Docker
  excludes.add("site/public/**")
  excludes.add("site/resources/_gen/**")
  excludes.add("node_modules/**")

  // Python
  excludes.add("**/.venv/**")
  excludes.add("**/polaris-venv/**")
  excludes.add("**/poetry.lock")
  excludes.add("**/.ruff_cache/**")
  excludes.add("**/.mypy_cache/**")
  excludes.add("**/.pytest_cache/**")
  excludes.add("client/python/.openapi-generator/**")

  // Jupyter
  excludes.add("**/*.ipynb")

  // regtests
  excludes.add("regtests/**/py.typed")
  excludes.add("regtests/**/*.ref")
  excludes.add("regtests/.env")
  excludes.add("regtests/derby.log")
  excludes.add("regtests/metastore_db/**")
  excludes.add("regtests/output/**")
  excludes.add("plugins/**/*.ref")

  // IntelliJ
  excludes.add(".idea")
  excludes.add("**/*.iml")
  excludes.add("**/*.iws")

  // Rat can't scan binary images
  excludes.add("**/*.png")
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

copiedCodeChecks {
  addDefaultContentTypes()

  licenseFile = project.layout.projectDirectory.file("LICENSE")

  scanDirectories {
    register("build-logic") { srcDir("build-logic/src") }
    register("misc") {
      srcDir(".github")
      srcDir("codestyle")
      srcDir("getting-started")
      srcDir("k8")
      srcDir("regtests")
      srcDir("server-templates")
      srcDir("spec")
    }
    register("gradle") {
      srcDir("gradle")
      exclude("wrapper/*.jar")
      exclude("wrapper/*.sha256")
    }
    register("site") {
      srcDir("site")
      exclude("build/**")
      exclude(".hugo_build.lock")
    }
    register("root") {
      srcDir(".")
      include("*")
    }
  }
}

changelog {
  repositoryUrl.set("https://github.com/apache/polaris")
  title.set("Apache Polaris Changelog")
  versionPrefix.set("apache-polaris-")
  header.set(provider { "${version.get()}" })
  groups.set(
    listOf(
      "Highlights",
      "Upgrade notes",
      "Breaking changes",
      "New Features",
      "Changes",
      "Deprecations",
      "Fixes",
      "Commits",
    )
  )
  version.set(provider { project.version.toString() })
}

tasks.register("showVersion") {
  actions.add {
    logger.lifecycle(
      "Polaris version is ${project.file("version.txt").readText(Charsets.UTF_8).trim()}"
    )
  }
}

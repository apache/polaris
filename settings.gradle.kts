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

import com.gradle.develocity.agent.gradle.scan.BuildScanPublishingConfiguration
import java.util.Properties
import org.gradle.api.configuration.BuildFeatures
import org.gradle.api.specs.Spec
import org.gradle.kotlin.dsl.support.serviceOf

val isCI = providers.environmentVariable("CI").isPresent

// Fail early and hard when Gradle's configuration cache is used in CI.
// Using the configuration cache in CI can leak secrets to the persisted configuration cache and
// from there anywhere.
if (isCI) {
  val configurationCacheRequested =
    gradle.serviceOf<BuildFeatures>().configurationCache.requested.getOrElse(false)
  if (configurationCacheRequested) {
    throw GradleException(
      "Gradle configuration cache must not be enabled in CI because it can persist build configuration state to disk."
    )
  }
}

includeBuild("build-logic") { name = "polaris-build-logic" }

includeBuild("gradle/server-test-runner") { name = "polaris-server-test-runner" }

if (!JavaVersion.current().isCompatibleWith(JavaVersion.VERSION_21)) {
  throw GradleException(
    """

        Build aborted...

        The Apache Polaris build requires Java 21.
        Detected Java version: ${JavaVersion.current()}

        """
  )
}

rootProject.name = "polaris"

val baseVersion =
  providers.fileContents(layout.settingsDirectory.file("version.txt")).asText.map { it.trim() }

gradle.beforeProject {
  version = baseVersion.get()
  group = "org.apache.polaris"
}

fun loadProperties(file: File): Properties {
  val props = Properties()
  file.reader().use { reader -> props.load(reader) }
  return props
}

fun polarisProject(name: String, directory: File) {
  include(name)
  val prj = project(":${name}")
  prj.name = name
  prj.projectDir = file(directory)
}

val projects = Properties()

loadProperties(file("gradle/projects.main.properties")).forEach { name, directory ->
  polarisProject(name as String, file(directory as String))
}

val ideaActive = providers.systemProperty("idea.active").getOrElse("false").toBoolean()

// load the polaris spark plugin projects
val polarisSparkDir = "plugins/spark"
val sparkScalaVersions = loadProperties(file("${polarisSparkDir}/spark-scala.properties"))
val sparkVersions = sparkScalaVersions["sparkVersions"].toString().split(",").map { it.trim() }

for (sparkVersion in sparkVersions) {
  val scalaVersionsKey = "scalaVersions.${sparkVersion}"
  val scalaVersionsStr = sparkScalaVersions[scalaVersionsKey].toString()
  val scalaVersions = scalaVersionsStr.split(",").map { it.trim() }
  var first = true
  for (scalaVersion in scalaVersions) {
    polarisProject(
      "polaris-spark-${sparkVersion}_${scalaVersion}",
      file("${polarisSparkDir}/v${sparkVersion}/spark"),
    )
    if (first) {
      first = false
    }
    // Skip all duplicated spark client projects while using IntelliJ IDE.
    // This is to avoid problems during dependency analysis and sync when
    // using IntelliJ, like "Multiple projects in this build have project directory".
    if (ideaActive) {
      break
    }
  }
}

pluginManagement {
  repositories {
    mavenCentral() // prefer Maven Central, in case Gradle's repo has issues
    gradlePluginPortal()
  }
}

plugins {
  // When updating the develocity plugin version, verify that the version that
  // https://develocity.apache.org/ runs is compatible with the plugin version
  // as on https://docs.gradle.com/develocity/current/miscellaneous/compatibility/
  id("com.gradle.develocity") version "4.4.3"
  id("com.gradle.common-custom-user-data-gradle-plugin") version "2.6.0"
}

dependencyResolutionManagement {
  repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
  repositories {
    mavenCentral()
    val useApacheSnapshots =
      providers.gradleProperty("useApacheSnapshots").orNull?.toBoolean() == true
    if (useApacheSnapshots) {
      // This is a hack to let Renovate _not_ query the Apache snapshot repository for all
      // dependencies.
      // See https://github.com/renovatebot/renovate/discussions/41291
      fun configureIndirectForRenovate(asfSnap: MavenArtifactRepository) {
        asfSnap.url = uri("https://repository.apache.org/content/repositories/snapshots/")
        asfSnap.mavenContent { snapshotsOnly() }
      }
      maven {
        name = "ApacheSnapshots"
        configureIndirectForRenovate(this)
      }
    }
    gradlePluginPortal()
  }
}

val isBuildScanRequested = gradle.startParameter.isBuildScan

develocity {
  val isApachePolarisGitHub =
    "apache/polaris" == providers.environmentVariable("GITHUB_REPOSITORY").orNull
  val gitHubRef: String? = providers.environmentVariable("GITHUB_REF").orNull
  val isGitHubBranchOrTag =
    gitHubRef != null && (gitHubRef.startsWith("refs/heads/") || gitHubRef.startsWith("refs/tags/"))
  if (isApachePolarisGitHub && isGitHubBranchOrTag) {
    // Use the ASF's Develocity instance when running against the Apache Polaris repository against
    // a branch or tag.
    // This is for CI runs that have access to the secret for the ASF's Develocity instance.
    server = "https://develocity.apache.org"
    projectId = "polaris"
    buildScan {
      uploadInBackground = !isCI
      publishing.onlyIf(AuthenticatedBuildScanPublishingSpec())
      obfuscation { ipAddresses { addresses -> addresses.map { _ -> "0.0.0.0" } } }
    }
  } else {
    // In all other cases, especially PR CI runs, use Gradle's public Develocity instance.
    projectId =
      providers
        .environmentVariable("DEVELOCITY_PROJECT_ID")
        .filter { it.isNotBlank() }
        .getOrElse("polaris")
    buildScan {
      val isGradleTosAccepted =
        "true" == providers.environmentVariable("GRADLE_TOS_ACCEPTED").orNull
      val isGitHubPullRequest = gitHubRef?.startsWith("refs/pull/") ?: false
      if (isGradleTosAccepted || (isCI && isGitHubPullRequest && isApachePolarisGitHub)) {
        // Leave TOS agreement to the user, if not running in CI.
        termsOfUseUrl = "https://gradle.com/terms-of-service"
        termsOfUseAgree = "yes"
      }
      providers
        .environmentVariable("DEVELOCITY_SERVER")
        .filter { it.isNotBlank() }
        .orNull
        ?.run { server = this }
      if (isGitHubPullRequest) {
        providers.environmentVariable("GITHUB_SERVER_URL").orNull?.run {
          val ghUrl = this
          val ghRepo = providers.environmentVariable("GITHUB_REPOSITORY").get()
          val prNumber = gitHubRef.substringAfter("refs/pull/").substringBefore("/merge")
          link("GitHub pull request", "$ghUrl/$ghRepo/pull/$prNumber")
        }
      }
      uploadInBackground = !isCI
      publishing.onlyIf(RequestedBuildScanPublishingSpec(isCI || isBuildScanRequested))
      obfuscation { ipAddresses { addresses -> addresses.map { _ -> "0.0.0.0" } } }
    }
  }
}

class AuthenticatedBuildScanPublishingSpec :
  Spec<BuildScanPublishingConfiguration.PublishingContext> {
  override fun isSatisfiedBy(context: BuildScanPublishingConfiguration.PublishingContext): Boolean =
    context.isAuthenticated
}

class RequestedBuildScanPublishingSpec(private val enabled: Boolean) :
  Spec<BuildScanPublishingConfiguration.PublishingContext> {
  override fun isSatisfiedBy(context: BuildScanPublishingConfiguration.PublishingContext): Boolean =
    enabled
}

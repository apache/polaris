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
import org.gradle.api.specs.Spec

// Fail early and hard when Gradle's configuration cache is used in CI.
// Using the configuration cache in CI can leak secrets to the persisted configuration cache and
// from there anywhere.
if (
  gradle.startParameter.isConfigurationCacheRequested &&
    providers.environmentVariable("CI").map(String::toBoolean).getOrElse(false)
) {
  throw GradleException(
    "Gradle configuration cache must not be enabled in CI because it can persist build configuration state to disk."
  )
}

includeBuild("build-logic") { name = "polaris-build-logic" }

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

val baseVersion = layout.rootDirectory.file("version.txt").asFile.readText().trim()

gradle.beforeProject {
  version = baseVersion
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
    val sparkArtifactId = "polaris-spark-${sparkVersion}_${scalaVersion}"
    val sparkIntArtifactId = "polaris-spark-integration-${sparkVersion}_${scalaVersion}"
    polarisProject(
      "polaris-spark-${sparkVersion}_${scalaVersion}",
      file("${polarisSparkDir}/v${sparkVersion}/spark"),
    )
    polarisProject(
      "polaris-spark-integration-${sparkVersion}_${scalaVersion}",
      file("${polarisSparkDir}/v${sparkVersion}/integration"),
    )
    if (first) {
      first = false
    }
    // Skip all duplicated spark client projects while using Intelij IDE.
    // This is to avoid problems during dependency analysis and sync when
    // using Intelij, like "Multiple projects in this build have project directory".
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
  id("com.gradle.develocity") version "4.4.2"
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

val isCI = System.getenv("CI") != null
val isBuildScanRequested = gradle.startParameter.isBuildScan

develocity {
  val isApachePolarisGitHub = "apache/polaris" == System.getenv("GITHUB_REPOSITORY")
  val gitHubRef: String? = System.getenv("GITHUB_REF")
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
    var cfgPrjId: String? = System.getenv("DEVELOCITY_PROJECT_ID")
    projectId = if (cfgPrjId.isNullOrEmpty()) "polaris" else cfgPrjId
    buildScan {
      val isGradleTosAccepted = "true" == System.getenv("GRADLE_TOS_ACCEPTED")
      val isGitHubPullRequest = gitHubRef?.startsWith("refs/pull/") ?: false
      if (isGradleTosAccepted || (isCI && isGitHubPullRequest && isApachePolarisGitHub)) {
        // Leave TOS agreement to the user, if not running in CI.
        termsOfUseUrl = "https://gradle.com/terms-of-service"
        termsOfUseAgree = "yes"
      }
      System.getenv("DEVELOCITY_SERVER")?.run {
        if (isNotEmpty()) {
          server = this
        }
      }
      if (isGitHubPullRequest) {
        System.getenv("GITHUB_SERVER_URL")?.run {
          val ghUrl = this
          val ghRepo = System.getenv("GITHUB_REPOSITORY")
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

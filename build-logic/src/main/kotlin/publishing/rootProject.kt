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

package publishing

import asf.AsfProject
import io.github.gradlenexus.publishplugin.NexusPublishExtension
import io.github.gradlenexus.publishplugin.NexusPublishPlugin
import io.github.gradlenexus.publishplugin.internal.StagingRepositoryDescriptorRegistryBuildService
import org.gradle.api.Project
import org.gradle.api.services.BuildServiceRegistration
import org.gradle.api.tasks.Exec
import org.gradle.kotlin.dsl.apply
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.register

/**
 * Configures Apache project specific publishing tasks on the root project, for example the
 * source-tarball related tasks.
 */
internal fun configureOnRootProject(project: Project) =
  project.run {
    apply<NexusPublishPlugin>()

    val isRelease = project.hasProperty("release")

    val sourceTarball = tasks.register<Exec>("sourceTarball")
    sourceTarball.configure {
      group = "build"
      description =
        "Generate a source tarball for a release to be uploaded to dist.apache.org/repos/dist"

      outputs.upToDateWhen { false }
      outputs.cacheIf { false }

      val e = project.extensions.getByType(PublishingHelperExtension::class.java)
      doFirst { mkdir(e.distributionDir) }

      // Use a fixed mtime for reproducible tarballs, using the same timestamp as jars do.
      // Also don't use the git-internal gzip as it's not stable, see
      // https://reproducible-builds.org/docs/archives/.
      commandLine =
        listOf(
          "bash",
          "-c",
          """
        git \
          archive \
          --prefix="${e.baseName.get()}/" \
          --format=tar \
          --mtime="1980-02-01 00:00:00 UTC" \
          HEAD | gzip -6 --no-name > "${e.sourceTarball.get().asFile.relativeTo(projectDir)}"
          """
            .trimIndent(),
        )
      workingDir(project.projectDir)

      outputs.file(e.sourceTarball)
    }

    digestTaskOutputs(sourceTarball)

    signTaskOutputs(sourceTarball)

    val releaseEmailTemplate = tasks.register("releaseEmailTemplate")
    releaseEmailTemplate.configure {
      group = "publishing"
      description =
        "Generate release-vote email subject + body, including the staging repository URL, if run during the Maven release."

      mustRunAfter("initializeApacheStagingRepository")

      // Capture project-derived values at configuration time to avoid
      // Task.getProject() at execution time (deprecated in Gradle 10).
      val publishingHelperExt = project.extensions.getByType(PublishingHelperExtension::class.java)
      val nexusPublishExt = project.extensions.getByType(NexusPublishExtension::class.java)
      val rootProject = project.rootProject
      val gradle = project.gradle
      val projectVersion = project.version

      doFirst {
        val asfName = publishingHelperExt.asfProjectId.get()

        val gitInfo = GitInfo.memoized(rootProject)
        val gitCommitId = gitInfo.gitHead
        val gitTag = gitInfo.gitDescribe
        val rcNumber = Regex("-rc(\\d+)$").find(gitTag)?.groupValues?.get(1) ?: "<RC_NUMBER>"

        val repos = nexusPublishExt.repositories
        val repo = repos.iterator().next()

        val stagingRepositoryUrlRegistryRegistration =
          gradle.sharedServices.registrations.named<
            BuildServiceRegistration<StagingRepositoryDescriptorRegistryBuildService, *>
          >(
            "stagingRepositoryUrlRegistry"
          )
        val stagingRepoUrl =
          if (stagingRepositoryUrlRegistryRegistration.isPresent) {
            val stagingRepositoryUrlRegistryBuildServiceRegistration =
              stagingRepositoryUrlRegistryRegistration.get()
            val stagingRepositoryUrlRegistryService =
              stagingRepositoryUrlRegistryBuildServiceRegistration.getService()
            if (stagingRepositoryUrlRegistryService.isPresent) {
              val registry = stagingRepositoryUrlRegistryService.get().registry
              try {
                val stagingRepoDesc = registry.get(repo.name)
                val stagingRepoId = stagingRepoDesc.stagingRepositoryId
                "https://repository.apache.org/content/repositories/$stagingRepoId/"
              } catch (e: IllegalStateException) {
                "NO STAGING REPOSITORY ($e)"
              }
            } else {
              "NO STAGING REPOSITORY (no registry service) !!"
            }
          } else {
            "NO STAGING REPOSITORY (no build service) !!"
          }

        val asfProject = AsfProject.memoized(rootProject, asfName)
        val asfProjectName =
          publishingHelperExt.overrideName.orElse("Apache ${asfProject.name}").get()

        val emailTemplatesDir = project.layout.buildDirectory.dir("email-templates").get().asFile
        emailTemplatesDir.mkdirs()
        val subjectFile =
          emailTemplatesDir.resolve("${publishingHelperExt.baseName.get()}.vote-email-subject.txt")
        val bodyFile =
          emailTemplatesDir.resolve("${publishingHelperExt.baseName.get()}.vote-email-body.txt")

        val emailSubject = "[VOTE] Release $asfProjectName $projectVersion (rc$rcNumber)"
        subjectFile.writeText(emailSubject)

        val emailBody =
          """
              Hi everyone,

              I propose that we release the following RC as the official $asfProjectName $version release.

              This corresponds to the tag: $gitTag
              * https://github.com/apache/$asfName/commits/$gitTag
              * https://github.com/apache/$asfName/tree/$gitCommitId

              The release tarball, signature, and checksums are here:
              * https://dist.apache.org/repos/dist/dev/$asfName/$projectVersion

              Helm charts are available on:
              * https://dist.apache.org/repos/dist/dev/$asfName/helm-chart/$projectVersion

              NB: you have to build the Docker images locally in order to test Helm charts.

              You can find the KEYS file here:
              * https://downloads.apache.org/$asfName/KEYS

              Convenience binary artifacts are staged on Nexus. The Maven repository URL is:
              * $stagingRepoUrl

              Please download, verify, and test according to the release verification guide, which can be found at:
              * https://polaris.apache.org/community/release-guides/release-verification-guide/

              Please vote in the next 72 hours.

              [ ] +1 Release this as Apache Polaris $projectVersion
              [ ] +0
              [ ] -1 Do not release this because...

              Only PMC members have binding votes, but other community members are encouraged to cast non-binding votes.
              This vote will pass if there are 3 binding +1 votes and more binding +1 votes than -1 votes.
            """

        logger.lifecycle(
          """


              The email for your release vote mail:
              -------------------------------------

              Suggested subject: (also in file $subjectFile)

              $emailSubject

              Suggested body: (also in file $bodyFile)

              $emailBody

              """
            .trimIndent()
        )
        bodyFile.writeText(emailBody.trimIndent())
      }
    }

    if (isRelease) {
      sourceTarball.configure { finalizedBy(releaseEmailTemplate) }
    }

    afterEvaluate {
      tasks.named("closeApacheStagingRepository") { mustRunAfter(releaseEmailTemplate) }
    }
  }

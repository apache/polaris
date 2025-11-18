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
          --mtime="1980-02-01 00:00:00" \
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

      doFirst {
        val e = project.extensions.getByType(PublishingHelperExtension::class.java)
        val asfName = e.asfProjectId.get()

        val gitCommitId = GitInfo.memoized(rootProject).gitHead

        val repos = project.extensions.getByType(NexusPublishExtension::class.java).repositories
        val repo = repos.iterator().next()

        val stagingRepositoryUrlRegistryRegistration =
          gradle.sharedServices.registrations.named<
            BuildServiceRegistration<StagingRepositoryDescriptorRegistryBuildService, *>
          >(
            "stagingRepositoryUrlRegistry"
          )
        val staginRepoUrl =
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

        val asfProject = AsfProject.memoized(project, asfName)
        val asfProjectName =
          e.overrideName.orElse(project.provider { "Apache ${asfProject.name}" }).get()

        val versionNoRc = version.toString().replace("-rc-?[0-9]+".toRegex(), "")

        val subjectFile = e.distributionFile("vote-email-subject.txt").relativeTo(projectDir)
        val bodyFile = e.distributionFile("vote-email-body.txt").relativeTo(projectDir)

        val emailSubject = "[VOTE] Release $asfProjectName $version"
        subjectFile.writeText(emailSubject)

        val emailBody =
          """
              Hi everyone,

              I propose that we release the following RC as the official
              $asfProjectName $versionNoRc release.

              * This corresponds to the tag: apache-$asfName-$version
              * https://github.com/apache/$asfName/commits/apache-$asfName-$version
              * https://github.com/apache/$asfName/tree/$gitCommitId

              The release tarball, signature, and checksums are here:
              * https://dist.apache.org/repos/dist/dev/incubator/$asfName/apache-$asfName-$version

              You can find the KEYS file here:
              * https://downloads.apache.org/incubator/$asfName/KEYS

              Convenience binary artifacts are staged on Nexus. The Maven repository URL is:
              * $staginRepoUrl

              Please download, verify, and test.

              Please vote in the next 72 hours.

              [ ] +1 Release this as Apache $asfName $version
              [ ] +0
              [ ] -1 Do not release this because...

              Only PPMC members and mentors have binding votes, but other community members are
              encouraged to cast non-binding votes. This vote will pass if there are
              3 binding +1 votes and more binding +1 votes than -1 votes.

              NB: if this vote pass, a new vote has to be started on the Incubator general mailing
              list.

              Thanks
              Regards
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

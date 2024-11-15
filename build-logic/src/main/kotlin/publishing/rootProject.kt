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

import io.github.gradlenexus.publishplugin.NexusPublishExtension
import io.github.gradlenexus.publishplugin.NexusPublishPlugin
import io.github.gradlenexus.publishplugin.internal.StagingRepositoryDescriptorRegistryBuildService
import org.gradle.api.Project
import org.gradle.api.services.BuildServiceRegistration
import org.gradle.kotlin.dsl.apply
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.register
import org.gradle.plugins.signing.Sign

/**
 * Configures Apache project specific publishing tasks on the root project, for example the
 * source-tarball related tasks.
 */
internal fun configureOnRootProject(project: Project) =
  project.run {
    apply<NexusPublishPlugin>()

    val isRelease = project.hasProperty("release")
    val isSigning = isRelease || project.hasProperty("signArtifacts")

    val sourceTarball = tasks.register("sourceTarball")
    sourceTarball.configure() {
      group = "build"
      description =
        "Generate a source tarball for a release to be uploaded to dist.apache.org/repos/dist"

      doFirst {
        val e = project.extensions.getByType(PublishingHelperExtension::class.java)
        mkdir(e.distributionDir)
        exec {
          executable = "git"
          args(
            "archive",
            "--prefix=${e.baseName.get()}/",
            "--format=tar.gz",
            "--output=${e.sourceTarball.get().asFile.relativeTo(projectDir)}",
            "HEAD"
          )
          workingDir(project.projectDir)
        }
      }
    }

    val digestSourceTarball = tasks.register("digestSourceTarball")
    digestSourceTarball.configure {
      mustRunAfter(sourceTarball)

      doFirst {
        val e = project.extensions.getByType(PublishingHelperExtension::class.java)
        generateDigest(e.sourceTarball.get().asFile, e.sourceTarballDigest.get().asFile, "SHA-512")
      }
    }

    sourceTarball.configure { finalizedBy(digestSourceTarball) }

    if (isSigning) {
      val signSourceTarball = tasks.register<Sign>("signSourceTarball")
      signSourceTarball.configure {
        mustRunAfter(sourceTarball)
        doFirst {
          val e = project.extensions.getByType(PublishingHelperExtension::class.java)
          sign(e.sourceTarball.get().asFile)
        }
      }
      sourceTarball.configure { finalizedBy(signSourceTarball) }
    }

    val releaseEmailTemplate = tasks.register("releaseEmailTemplate")
    releaseEmailTemplate.configure {
      group = "publishing"
      description =
        "Generate release-vote email subject + body, including the staging repository URL, if run during the Maven release."

      mustRunAfter("initializeApacheStagingRepository")

      doFirst {
        val e = project.extensions.getByType(PublishingHelperExtension::class.java)
        val asfName = e.asfProjectName.get()

        val gitInfo = MemoizedGitInfo.gitInfo(rootProject)
        val gitCommitId = gitInfo["Apache-Polaris-Build-Git-Head"]

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

        val (asfPrj, _) = fetchAsfProject(asfName)
        val asfProjectName = asfPrj["name"] as String

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
              * https://dist.apache.org/repos/dist/release/incubator/$asfName/KEYS

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

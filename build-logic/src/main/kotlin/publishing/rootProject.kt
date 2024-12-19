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

import io.github.gradlenexus.publishplugin.NexusPublishPlugin
import org.gradle.api.Project
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.Exec
import org.gradle.kotlin.dsl.apply
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

    val cleanDistributionsDir = tasks.register<Delete>("cleanDistributionsDir")
    cleanDistributionsDir.configure {
      outputs.cacheIf { false }

      val e = project.extensions.getByType(PublishingHelperExtension::class.java)
      delete(e.distributionDir)
    }

    val sourceTarball = tasks.register<Exec>("sourceTarball")
    sourceTarball.configure {
      group = "build"
      description =
        "Generate a source tarball for a release to be uploaded to dist.apache.org/repos/dist"
      outputs.cacheIf { false }

      dependsOn(cleanDistributionsDir)

      val e = project.extensions.getByType(PublishingHelperExtension::class.java)
      doFirst { mkdir(e.distributionDir) }

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
  }

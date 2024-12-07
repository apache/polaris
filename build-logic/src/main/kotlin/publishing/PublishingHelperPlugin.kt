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

import com.github.jengelman.gradle.plugins.shadow.ShadowExtension
import javax.inject.Inject
import org.gradle.api.*
import org.gradle.api.component.SoftwareComponentFactory
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin
import org.gradle.api.publish.tasks.GenerateModuleMetadata
import org.gradle.jvm.tasks.Jar
import org.gradle.kotlin.dsl.*
import org.gradle.plugins.signing.SigningExtension
import org.gradle.plugins.signing.SigningPlugin

/**
 * Release-publishing helper plugin to generate publications that pass Sonatype validations,
 * generate Apache release source tarball.
 *
 * The `release` Gradle project property triggers: signed artifacts + jars with Git information. The
 * current Git HEAD must point to a Git tag.
 *
 * The `jarWithGitInfo` Gradle project property triggers: jars with Git information (not necessary
 * with `release`).
 *
 * The task `sourceTarball` (available on the root project) generates a source tarball using `git
 * archive`.
 *
 * Signing tip: If you want to use `gpg-agent`, set the `useGpgAgent` Gradle project property
 *
 * ```
 * ./gradlew publishToMavenLocal sourceTarball -Prelease -PuseGpgAgent
 * ```
 *
 * You can generate signed artifacts when using the `signArtifacts` project property:
 * ```
 * ./gradlew publishToMavenLocal sourceTarball -PsignArtifacts -PuseGpgAgent
 * ```
 */
@Suppress("unused")
class PublishingHelperPlugin
@Inject
constructor(private val softwareComponentFactory: SoftwareComponentFactory) : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {
      extensions.create("publishingHelper", PublishingHelperExtension::class.java)

      val isRelease = project.hasProperty("release")
      val isSigning = isRelease || project.hasProperty("signArtifacts")

      // Adds Git/Build/System related information to the generated jars, if the `release` project
      // property is present. Do not add that information in development builds, so that the
      // generated jars are still cacheable for Gradle.
      if (isRelease || project.hasProperty("jarWithGitInfo")) {
        // Runs `git`, considered expensive, so guarded behind project properties.
        tasks.withType<Jar>().configureEach {
          manifest { MemoizedGitInfo.gitInfo(rootProject, attributes) }
        }
      }

      apply(plugin = "maven-publish")
      apply(plugin = "signing")

      // Generate a source tarball for a release to be uploaded to
      // https://dist.apache.org/repos/dist/dev/<name>/apache-<name>-<version-with-rc>/
      if (project == rootProject) {
        configureOnRootProject(project)
      }

      if (isSigning) {
        plugins.withType<SigningPlugin>().configureEach {
          configure<SigningExtension> {
            val signingKey: String? by project
            val signingPassword: String? by project
            useInMemoryPgpKeys(signingKey, signingPassword)
            val publishing = project.extensions.getByType(PublishingExtension::class.java)
            afterEvaluate { sign(publishing.publications.getByName("maven")) }

            if (project.hasProperty("useGpgAgent")) {
              useGpgCmd()
            }
          }
        }
      }

      // Gradle complains when a Gradle module metadata ("pom on steroids") is generated with an
      // enforcedPlatform() dependency - but Quarkus requires enforcedPlatform(), so we have to
      // allow it.
      tasks.withType<GenerateModuleMetadata>().configureEach {
        suppressedValidationErrors.add("enforced-platform")
      }

      plugins.withType<MavenPublishPlugin>().configureEach {
        configure<PublishingExtension> {
          publications {
            register<MavenPublication>("maven") {
              val mavenPublication = this
              afterEvaluate {
                // This MUST happen in an 'afterEvaluate' to ensure that the Shadow*Plugin has
                // been applied.
                if (project.extensions.findByType(ShadowExtension::class.java) != null) {
                  configureShadowPublishing(project, mavenPublication, softwareComponentFactory)
                } else {
                  from(components.firstOrNull { c -> c.name == "javaPlatform" || c.name == "java" })
                }

                suppressPomMetadataWarningsFor("testFixturesApiElements")
                suppressPomMetadataWarningsFor("testFixturesRuntimeElements")

                mavenPublication.groupId = "$group"
                mavenPublication.version = project.version.toString()
              }

              tasks.named("generatePomFileForMavenPublication").configure {
                configurePom(project, mavenPublication, this)
              }
            }
          }
        }
      }
    }
}

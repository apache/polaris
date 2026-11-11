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

package org.apache.polaris.gradle.licenses

import app.cash.licensee.LicenseeExtension
import app.cash.licensee.LicenseePlugin
import app.cash.licensee.LicenseeTask
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.publish.maven.tasks.AbstractPublishToMaven
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.register

class LicenseChecksPlugin : Plugin<Project> {
  override fun apply(project: Project) {
    project.pluginManager.apply(LicenseePlugin::class.java)
    val extension =
      project.extensions.create(
        "licenseChecks",
        LicenseChecksExtension::class.java,
        project.providers,
        project.extensions.getByType<LicenseeExtension>(),
      )
    extension.dependencyConfiguration.convention("quarkusProdRuntimeClasspathConfiguration")
    extension.inventoryTask.convention("quarkusAppPartsBuild")
    extension.generatedComponentNames.convention(
      setOf(
        "generated-bytecode.jar",
        "quarkus-app-dependencies.txt",
        "quarkus-application.dat",
        "quarkus-run.jar",
        "transformed-bytecode.jar",
      ),
    )
    extension.sbom.convention(project.layout.buildDirectory.file("quarkus-run-cyclonedx.json"))
    extension.licenseFile.convention(project.layout.projectDirectory.file("distribution/LICENSE"))
    extension.noticeFile.convention(project.layout.projectDirectory.file("distribution/NOTICE"))
    extension.projectGroup.convention("org.apache.polaris")
    extension.allowUnusedLicenseMarkers.convention(false)

    project.pluginManager.withPlugin("io.quarkus") {
      project.tasks.named("quarkusAppPartsBuild") {
        // Quarkus generates this beside quarkus-app, outside its declared cached outputs.
        // Restore the inventory together with the application instead of leaving stale/missing
        // data.
        outputs
          .file(project.layout.buildDirectory.file("quarkus-run-cyclonedx.json"))
          .withPropertyName("cyclonedxSbom")
      }
    }

    val licensee =
      project.tasks.named<LicenseeTask>("licensee") {
        configurationToCheck(project.configurations.named(extension.dependencyConfiguration.get()))
        dependsOn(extension.inventoryTask)
      }
    val verification =
      project.tasks.register<VerifyLicenseAttribution>("verifyLicenseAttribution") {
        group = "verification"
        description =
          "Verifies that shipped dependencies have Licensee checks and LICENSE attribution"
        licenseeReport.set(licensee.flatMap { it.outputDir.file("artifacts.json") })
        sbom.set(extension.sbom)
        licenseFile.set(extension.licenseFile)
        noticeFile.set(extension.noticeFile)
        projectGroup.set(extension.projectGroup)
        generatedComponentNames.set(extension.generatedComponentNames)
        allowUnusedLicenseMarkers.set(extension.allowUnusedLicenseMarkers)
        validationReport.set(
          project.layout.buildDirectory.file("reports/licensee/attribution-validation.txt"),
        )
      }
    val checks =
      project.tasks.register("checkDistributionLicenses") {
        group = "verification"
        description = "Checks distribution dependency licenses and attribution"
        dependsOn(verification)
      }
    project.tasks.named("check") { dependsOn(checks) }
    project.tasks.withType(AbstractPublishToMaven::class.java).configureEach { dependsOn(checks) }
  }
}

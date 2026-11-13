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

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.bundling.Tar
import org.gradle.api.tasks.bundling.Zip
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.register

class DistributionLicenseChecksPlugin : Plugin<Project> {
  override fun apply(project: Project) {
    val merged = project.layout.buildDirectory.dir("merged-license-notice")
    val zip = project.tasks.named<Zip>("distZip")
    val tar = project.tasks.named<Tar>("distTar")
    val archives =
      mapOf(
        "Zip" to project.zipTree(zip.flatMap { it.archiveFile }),
        "Tar" to project.tarTree(tar.flatMap { it.archiveFile }),
      )
    val checks =
      project.tasks.register("checkDistributionLicenses") {
        group = "verification"
        description = "Checks license policy and attribution delivered in binary distributions"
      }
    archives.forEach { (kind, contents) ->
      val archive = if (kind == "Zip") zip else tar
      val verification =
        project.tasks.register<VerifyDistributionAttribution>("verifyDist${kind}Attribution") {
          group = "verification"
          description = "Verifies LICENSE and NOTICE delivered in the binary $kind"
          dependsOn(archive, "licenseNoticeMerge")
          licenseFile.set(merged.map { it.file("LICENSE") })
          noticeFile.set(merged.map { it.file("NOTICE") })
          deliveredLicense.from(contents.matching { include("*/LICENSE") })
          deliveredNotice.from(contents.matching { include("*/NOTICE") })
          validationReport.set(
            project.layout.buildDirectory.file(
              "reports/licensee/dist-${kind.lowercase()}-attribution.txt",
            ),
          )
        }
      archive.configure { finalizedBy(verification) }
      checks.configure { dependsOn(verification) }
    }
    project.tasks.named("check") { dependsOn(checks) }
  }
}

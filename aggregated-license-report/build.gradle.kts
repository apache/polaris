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

import org.gradle.kotlin.dsl.support.unzipTo

val licenseReports by configurations.creating { description = "Used to generate license reports" }

dependencies {
  licenseReports(project(":polaris-service", "licenseReports"))
}

val collectLicenseReportJars by
  tasks.registering(Sync::class) {
    destinationDir = project.layout.buildDirectory.dir("tmp/license-report-jars").get().asFile
    from(licenseReports)
  }

val aggregateLicenseReports by
  tasks.registering {
    group = "Build"
    description = "Aggregates license reports"
    val outputDir = project.layout.buildDirectory.dir("licenseReports")
    outputs.dir(outputDir)
    dependsOn(collectLicenseReportJars)
    doLast {
      delete(outputDir)
      fileTree(collectLicenseReportJars.get().destinationDir).files.forEach { zip ->
        val targetDirName = zip.name.replace("-license-report.zip", "")
        unzipTo(outputDir.get().dir(targetDirName).asFile, zip)
      }
    }
  }

val aggregatedLicenseReportsZip by
  tasks.registering(Zip::class) {
    from(aggregateLicenseReports)
    from(rootProject.layout.projectDirectory) {
      include("NOTICE", "LICENSE")
      eachFile {
        path = file.name + ".txt"
      }
    }
    archiveBaseName.set("polaris-aggregated-license-report-${project.version}")
    destinationDirectory.set(layout.buildDirectory.dir("distributions"))
    archiveExtension.set("zip")
  }

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

import com.github.jk1.license.filter.LicenseBundleNormalizer
import com.github.jk1.license.render.InventoryHtmlReportRenderer
import com.github.jk1.license.render.JsonReportRenderer
import com.github.jk1.license.render.ReportRenderer
import com.github.jk1.license.render.XmlReportRenderer
import com.github.jk1.license.task.ReportTask
import licenses.LicenseFileValidation
import licenses.NoticeFileLicenseFilter
import licenses.QuarkusAppDependencyFilter

plugins { id("com.github.jk1.dependency-license-report") }

val normalizerBundle = layout.settingsDirectory.file("gradle/license/normalizer-bundle.json")
val allowedLicenses = layout.settingsDirectory.file("gradle/license/allowed-licenses.json")

licenseReport {
  filters =
    arrayOf(
      QuarkusAppDependencyFilter(),
      LicenseBundleNormalizer(normalizerBundle.asFile.absolutePath, false),
      NoticeFileLicenseFilter(),
      LicenseFileValidation(),
    )
  allowedLicensesFile = allowedLicenses.asFile
  renderers =
    arrayOf<ReportRenderer>(
      InventoryHtmlReportRenderer("index.html"),
      JsonReportRenderer(),
      XmlReportRenderer(),
    )
  excludeBoms = true
  outputDir = layout.buildDirectory.dir("reports/dependency-license").get().asFile.absolutePath
  configurations = arrayOf("quarkusProdRuntimeClasspathConfiguration")
}

val generateLicenseReport =
  tasks.named<ReportTask>("generateLicenseReport") {
    dependsOn("quarkusBuild")
    inputs
      .files(
        normalizerBundle,
        allowedLicenses,
        project.layout.buildDirectory.file("quarkus-app/quarkus-app-dependencies.txt"),
      )
      .withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.property("renderersHash", licenseReport.renderers.contentHashCode())
    inputs.property("filtersHash", licenseReport.filters.contentHashCode())
    inputs.property("excludesHash", licenseReport.excludes.contentHashCode())
    inputs.property("excludeGroupsHash", licenseReport.excludeGroups.contentHashCode())
  }

val licenseReportZip =
  tasks.register<Zip>("licenseReportZip") {
    group = "documentation"
    description = "License report as a ZIP"
    dependsOn("checkLicense")
    from(generateLicenseReport)
    archiveClassifier.set("license-report")
    archiveExtension.set("zip")
  }

val licenseReports by
  configurations.creating {
    isCanBeConsumed = true
    isCanBeResolved = false
    description = "License report files"
    outgoing { artifact(licenseReportZip) }
  }

plugins.withType<MavenPublishPlugin>().configureEach {
  configure<PublishingExtension> {
    publications { named<MavenPublication>("maven") { artifact(licenseReportZip) } }
  }
}

tasks.named("check") { dependsOn(generateLicenseReport) }

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
import com.github.jk1.license.render.XmlReportRenderer
import java.util.*

plugins { id("com.github.jk1.dependency-license-report") }

afterEvaluate {
  licenseReport {
    filters =
      arrayOf(
        LicenseBundleNormalizer(
          "${rootProject.projectDir}/gradle/license/normalizer-bundle.json",
          false
        ),
        LicenseFileValidation()
      )
    allowedLicensesFile = rootProject.projectDir.resolve("gradle/license/allowed-licenses.json")
    renderers =
      arrayOf(InventoryHtmlReportRenderer("index.html"), JsonReportRenderer(), XmlReportRenderer())
    excludeBoms = true
    outputDir = "${project.layout.buildDirectory.get()}/reports/dependency-license"
  }
}

val generateLicenseReport =
  tasks.named("generateLicenseReport") {
    inputs
      .files(
        rootProject.projectDir.resolve("gradle/license/normalizer-bundle.json"),
        rootProject.projectDir.resolve("gradle/license/allowed-licenses.json")
      )
      .withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.property("renderersHash", Arrays.hashCode(licenseReport.renderers))
    inputs.property("filtersHash", Arrays.hashCode(licenseReport.filters))
    inputs.property("excludesHash", Arrays.hashCode(licenseReport.excludes))
    inputs.property("excludeGroupsHash", Arrays.hashCode(licenseReport.excludeGroups))
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

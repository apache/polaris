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

import org.apache.polaris.gradle.licenses.LicenseChecksExtension
import org.apache.polaris.gradle.licenses.LicenseChecksPlugin
import org.apache.polaris.gradle.licenses.VerifyDistributionAttribution

// Keep Licensee separate from the application/build-logic plugin classpath.
buildscript {
  repositories {
    mavenCentral()
    gradlePluginPortal()
  }
  dependencies { classpath("org.apache.polaris.build:polaris-license-check-plugin") }
}

apply<LicenseChecksPlugin>()

extensions.configure<LicenseChecksExtension>("licenseChecks") {
  dependencyConfiguration.set("runtimeClasspath")
  inventoryTask.set("generateLicenseInventory")
  generatedComponentNames.set(emptySet())
  sbom.set(layout.buildDirectory.file("reports/licensee/shadow-inputs.json"))
  licenseFile.set(layout.projectDirectory.file("BUNDLE-LICENSE"))
  noticeFile.set(layout.projectDirectory.file("BUNDLE-NOTICE"))
  policyFrom(layout.settingsDirectory.file("gradle/license-policy.json"))
}

val verifyBundleAttribution =
  tasks.register<VerifyDistributionAttribution>("verifyBundleAttribution") {
    val bundle = tasks.named<AbstractArchiveTask>("createPolarisSparkJar")
    val contents = zipTree(bundle.flatMap { it.archiveFile })
    group = "verification"
    description = "Verifies LICENSE and NOTICE delivered in the Spark bundle"
    dependsOn(bundle)
    licenseFile.set(layout.projectDirectory.file("BUNDLE-LICENSE"))
    noticeFile.set(layout.projectDirectory.file("BUNDLE-NOTICE"))
    deliveredLicense.from(contents.matching { include("META-INF/LICENSE") })
    deliveredNotice.from(contents.matching { include("META-INF/NOTICE") })
    validationReport.set(layout.buildDirectory.file("reports/licensee/bundle-attribution.txt"))
  }
val checks = tasks.named("checkDistributionLicenses") { dependsOn(verifyBundleAttribution) }

tasks.withType<AbstractArchiveTask>().configureEach {
  if (name == "createPolarisSparkJar") {
    finalizedBy(checks)
  }
}

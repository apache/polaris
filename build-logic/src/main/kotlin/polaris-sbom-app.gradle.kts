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

import java.util.Base64
import org.cyclonedx.gradle.CyclonedxDirectTask
import org.cyclonedx.model.AttachmentText
import org.cyclonedx.model.Component.Type.APPLICATION
import org.cyclonedx.model.License
import org.cyclonedx.model.LicenseChoice
import org.cyclonedx.model.Property
import sbom.CyclonedxBundleTask
import sbom.createCyclonedxConfigurations

if (project.plugins.hasPlugin("io.quarkus")) {
  // See https://quarkus.io/guides/cyclonedx#gradle-dependency-sboms
  tasks.named<CyclonedxDirectTask>("cyclonedxDirectBom").configure {
    includeConfigs.addAll("quarkusProdRuntimeClasspathConfiguration")
  }
}

val cyclonedxApplicationBomTask =
  tasks.register<CyclonedxBundleTask>("cyclonedxApplicationBom") {
    group = "publishing"
    description = "Generate CycloneDX SBOMs for this as an application"

    val cyclonedxDirectBom = tasks.named<CyclonedxDirectTask>("cyclonedxDirectBom")
    inputBoms.from(cyclonedxDirectBom.map { it.jsonOutput })

    // want to include the original license text in the generated SBOM as it may include variations
    // from the "standard" license text
    includeLicenseText = true

    // build system information and serial number break reproducible builds
    includeBuildSystem = false
    includeBomSerialNumber = false

    projectType = APPLICATION

    // Needed for projects that use subdirectories in their build/ directory, like the Spark plugin
    jsonOutput.set(project.layout.buildDirectory.file("reports/cyclonedx-app/bom.json"))
    xmlOutput.set(project.layout.buildDirectory.file("reports/cyclonedx-app/bom.xml"))

    val relativeProjectDir = project.projectDir.relativeTo(project.rootProject.projectDir)
    val gitInfo = GitInfo.memoized(project)

    licenseChoice.set(
      LicenseChoice().apply {
        addLicense(
          License().apply {
            val gitCommit = GitInfo.memoized(project).gitHead
            id = "Apache-2.0"
            // TODO URL or text ??
            url = gitInfo.rawGithubLink("$relativeProjectDir/distribution/LICENSE")
            setLicenseText(
              AttachmentText().apply() {
                contentType = "plain/text"
                encoding = "base64"
                text =
                  Base64.getEncoder()
                    .encodeToString(project.file("distribution/LICENSE").readBytes())
              }
            )

            // TODO Is there a better way to include NOTICE + DISCLAIMER in a CycloneDX SBOM?
            val props = mutableListOf<Property>()
            props.add(
              Property().apply {
                name = "NOTICE"
                value =
                  project.file("distribution/NOTICE").readText(Charsets.UTF_8).replace("\n", "\\n")
              }
            )
            properties = props
          }
        )
      }
    )
  }

createCyclonedxConfigurations(project, cyclonedxApplicationBomTask)

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
import org.cyclonedx.model.AttachmentText
import org.cyclonedx.model.License
import org.cyclonedx.model.LicenseChoice
import org.cyclonedx.model.Property
import publishing.digestTaskOutputs
import publishing.signTaskOutputs
import sbom.CyclonedxBundleTask
import sbom.createCyclonedxConfigurations

plugins { id("org.cyclonedx.bom") }

val bundleSboms by
  configurations.creating {
    isCanBeConsumed = false
    isCanBeResolved = true
  }

val cyclonedxBundleBom = tasks.register<CyclonedxBundleTask>("cyclonedxBundleBom")

cyclonedxBundleBom.configure {
  inputBoms = bundleSboms
  // The distribution itself has no dependencies, just components
  includeDependencies = false

  // build system information and serial number break reproducible builds
  includeBuildSystem = false
  includeBomSerialNumber = false

  val relativeProjectDir = project.projectDir.relativeTo(project.rootProject.projectDir)
  val gitInfo = GitInfo.memoized(project)

  licenseChoice.set(
    LicenseChoice().apply {
      addLicense(
        License().apply {
          id = "Apache-2.0"
          // TODO URL or text ??
          url = gitInfo.rawGithubLink("$relativeProjectDir/LICENSE")
          setLicenseText(
            AttachmentText().apply() {
              contentType = "plain/text"
              encoding = "base64"
              text = Base64.getEncoder().encodeToString(project.file("LICENSE").readBytes())
            }
          )

          // TODO Is there a better way to include NOTICE + DISCLAIMER in a CycloneDX SBOM?
          val props = mutableListOf<Property>()
          props.add(
            Property().apply {
              name = "NOTICE"
              value = project.file("NOTICE").readText(Charsets.UTF_8).replace("\n", "\\n")
            }
          )
          properties = props
        }
      )
    }
  )
}

createCyclonedxConfigurations(project, cyclonedxBundleBom)

tasks.named("assemble") { dependsOn(cyclonedxBundleBom) }

digestTaskOutputs(cyclonedxBundleBom)

signTaskOutputs(cyclonedxBundleBom)

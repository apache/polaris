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

import gradle.kotlin.dsl.accessors._acd318faec9c69039294e12a9af5cd7d.publishing
import org.cyclonedx.gradle.BaseCyclonedxTask
import org.cyclonedx.gradle.CyclonedxAggregateTask
import org.cyclonedx.gradle.CyclonedxDirectTask
import org.cyclonedx.model.ExternalReference
import org.cyclonedx.model.License
import org.cyclonedx.model.LicenseChoice
import org.cyclonedx.model.OrganizationalEntity
import org.gradle.api.Project
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.kotlin.dsl.named

/**
 * Configures [CycloneDX plugin tasks](https://github.com/CycloneDX/cyclonedx-gradle-plugin) for the
 * Apache Polaris project.
 *
 * The `cyclonedxBom` task, only available on the root Gradle project, generates an aggregated BOM
 * for the project and all its modules.
 *
 * The `cyclonedxDirectBom` task generates the SBOM for the projects' main artifacts. The SBOM for
 * each published Maven artifact uses the classifier `cyclonedx` and is published as JSON and XML.
 */
fun Project.configureCycloneDx() {
  val cyclonedxBom = tasks.named<CyclonedxAggregateTask>("cyclonedxBom")
  cyclonedxBom.configure {
    if (project == rootProject) {
      configureForPolaris()
    } else {
      // Disable aggregation for subprojects
      enabled = false
    }

    // Needed for projects that use subdirectories in their build/ directory, like the Spark plugin
    jsonOutput.set(layout.buildDirectory.file("reports/cyclonedx/bom.json"))
    xmlOutput.set(layout.buildDirectory.file("reports/cyclonedx/bom.xml"))
  }

  val cyclonedxDirectBom = tasks.named<CyclonedxDirectTask>("cyclonedxDirectBom")
  cyclonedxDirectBom.configure {
    includeConfigs.addAll("runtimeClasspath")
    configureForPolaris()

    // Needed for projects that use subdirectories in their build/ directory, like the Spark plugin
    jsonOutput.set(layout.buildDirectory.file("reports/cyclonedx-direct/bom.json"))
    xmlOutput.set(layout.buildDirectory.file("reports/cyclonedx-direct/bom.xml"))
  }

  publishing {
    publications {
      named<MavenPublication>("maven") {
        artifact(cyclonedxDirectBom.map { t -> t.jsonOutput }) {
          classifier = "cyclonedx"
          builtBy(cyclonedxDirectBom)
        }
        artifact(cyclonedxDirectBom.map { t -> t.xmlOutput }) {
          classifier = "cyclonedx"
          builtBy(cyclonedxDirectBom)
        }
      }
    }
  }
}

internal fun BaseCyclonedxTask.configureForPolaris() {
  organizationalEntity.set(
    OrganizationalEntity().apply {
      name = "The Apache Software Foundation"
      urls = listOf("https://apache.org/")
    }
  )
  externalReferences.add(
    ExternalReference().apply {
      type = ExternalReference.Type.VCS
      url = "https://github.com/apache/polaris.git"
    }
  )
  externalReferences.add(
    ExternalReference().apply {
      type = ExternalReference.Type.WEBSITE
      url = "https://polaris.apache.org/"
    }
  )
  externalReferences.add(
    ExternalReference().apply {
      type = ExternalReference.Type.ISSUE_TRACKER
      url = "https://github.com/apache/polaris/issues"
    }
  )
  externalReferences.add(
    ExternalReference().apply {
      type = ExternalReference.Type.MAILING_LIST
      url = "https://lists.apache.org/list.html?dev@polaris.apache.org"
    }
  )
  externalReferences.add(
    ExternalReference().apply {
      type = ExternalReference.Type.SECURITY_CONTACT
      url = "mailto:security@apache.org"
    }
  )
  licenseChoice.set(
    LicenseChoice().apply {
      addLicense(
        License().apply {
          id = "Apache-2.0"
          url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
        }
      )
    }
  )
}

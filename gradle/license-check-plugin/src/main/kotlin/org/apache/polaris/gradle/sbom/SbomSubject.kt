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

package org.apache.polaris.gradle.sbom

import com.github.packageurl.PackageURLBuilder.aPackageURL
import org.cyclonedx.model.Component
import org.cyclonedx.model.ExternalReference
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Internal

class SbomSubject
@JvmOverloads
constructor(
  @get:Input val group: String,
  @get:Input val name: String,
  @get:Input val version: String,
  @get:Input val packageKind: PackageKind,
  @get:Input val classifier: String = "",
) {
  fun applyToComponent(component: Component) {
    component.group = group
    component.name = name
    component.version = version
    component.purl = purl
    component.bomRef = purl
    // The subject's source repository must not depend on the checkout remote or GIT_URL.
    component.externalReferences =
      component.externalReferences.orEmpty().filter { it.type != ExternalReference.Type.VCS } +
        ExternalReference().apply {
          type = ExternalReference.Type.VCS
          url = "https://github.com/apache/polaris"
        }
  }

  @get:Internal
  val purl: String
    get() =
      aPackageURL()
        .withType(if (packageKind == PackageKind.SHADED_JAR) "maven" else "generic")
        .withNamespace(if (packageKind == PackageKind.SHADED_JAR) group else null)
        .withName(name)
        .withVersion(version)
        .apply {
          if (packageKind == PackageKind.SHADED_JAR) {
            withQualifier("type", "jar")
            if (classifier.isNotEmpty()) {
              withQualifier("classifier", classifier)
            }
          } else {
            withQualifier("variant", packageKind.type)
          }
        }
        .build()
        .canonicalize()

  @get:Internal
  val sbomClassifier: String
    get() = if (classifier.isNotEmpty()) "$classifier-sbom-cyclonedx" else "sbom-cyclonedx"

  @get:Internal
  val sbomFilename: String
    get() =
      if (packageKind == PackageKind.SHADED_JAR)
        "$nameAndVersion${if (classifier.isEmpty()) "" else "-$classifier"}.jar.cdx.json"
      else "$nameAndVersion${packageKind.sbomBasenameSuffix}"

  @get:Internal
  val nameAndVersion: String
    get() = "$name-$version"
}

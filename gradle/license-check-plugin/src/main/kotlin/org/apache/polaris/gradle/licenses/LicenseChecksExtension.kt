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
import javax.inject.Inject
import org.gradle.api.file.RegularFile
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.Property
import org.gradle.api.provider.ProviderFactory
import org.gradle.api.provider.SetProperty

abstract class LicenseChecksExtension
@Inject
constructor(
  private val providers: ProviderFactory,
  private val licensee: LicenseeExtension,
) {
  abstract val dependencyConfiguration: Property<String>
  abstract val inventoryTask: Property<String>
  abstract val generatedComponentNames: SetProperty<String>
  abstract val sbom: RegularFileProperty
  abstract val licenseFile: RegularFileProperty
  abstract val noticeFile: RegularFileProperty
  abstract val projectGroup: Property<String>

  /** Loads shared policy at configuration time. Later Licensee DSL calls can add allowances. */
  fun policyFrom(file: RegularFile) {
    val contents = providers.fileContents(file).asText.get()
    val policy = LicensePolicy(contents, file.asFile)
    policy.allowedLicenses.forEach { licensee.allow(it) }
    policy.allowedUrls.forEach { entry ->
      licensee.allowUrl(entry.getValue("url")) { because(entry.getValue("reason")) }
    }
    policy.allowedDependencies.forEach { entry ->
      licensee.allowDependency(
        entry.getValue("group"),
        entry.getValue("artifact"),
        entry.getValue("version"),
      ) {
        because(entry.getValue("reason"))
      }
    }
  }
}

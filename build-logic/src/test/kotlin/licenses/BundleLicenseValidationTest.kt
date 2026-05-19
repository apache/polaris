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

package licenses

import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class BundleLicenseValidationTest {

  @Test
  fun validateLicenseMentions_passesWhenAllArtifactsMentioned() {
    val licenseText =
      """
      * Maven group:artifact IDs: com.example:foo_2.12
      * Maven group:artifact IDs: com.example:foo_2.13
      """
        .trimIndent()

    val error =
      BundleLicenseValidation.validateLicenseMentions(
        licenseText,
        bundledArtifacts = setOf("com.example:foo_2.12"),
        allowedExtraArtifacts = setOf("com.example:foo_2.13"),
      )

    assertNull(error)
  }

  @Test
  fun validateLicenseMentions_failsOnMissingArtifact() {
    val error =
      BundleLicenseValidation.validateLicenseMentions(
        licenseText = "* Maven group:artifact IDs: com.example:other\n",
        bundledArtifacts = setOf("com.example:foo_2.12"),
        allowedExtraArtifacts = emptySet(),
      )

    assertNotNull(error)
    assert(error!!.contains("com.example:foo_2.12"))
  }

  @Test
  fun validateLicenseMentions_failsOnSuperfluousMention() {
    val error =
      BundleLicenseValidation.validateLicenseMentions(
        licenseText =
          """
          * Maven group:artifact IDs: com.example:foo_2.12
          * Maven group:artifact IDs: com.example:stale
          """
            .trimIndent(),
        bundledArtifacts = setOf("com.example:foo_2.12"),
        allowedExtraArtifacts = emptySet(),
      )

    assertNotNull(error)
    assert(error!!.contains("com.example:stale"))
  }
}

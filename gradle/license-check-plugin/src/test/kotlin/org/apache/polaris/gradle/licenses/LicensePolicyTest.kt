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

import java.io.File
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource

class LicensePolicyTest {
  @ParameterizedTest
  @MethodSource("invalidPolicies")
  fun rejectsInvalidPolicy(policy: String, message: String) {
    assertThatThrownBy { LicensePolicy(policy, File("policy.json")) }.hasMessageContaining(message)
  }

  @Test
  fun absentSectionsAreEmpty() {
    val policy = LicensePolicy("{}", File("policy.json"))
    assertThat(policy.allowedLicenses).isEmpty()
    assertThat(policy.allowedUrls).isEmpty()
    assertThat(policy.allowedDependencies).isEmpty()
  }

  companion object {
    @JvmStatic
    fun invalidPolicies(): List<Arguments> =
      listOf(
        Arguments.of("[]", "License policy must be a JSON object"),
        Arguments.of("{", "Invalid license policy JSON"),
        Arguments.of("""{"allowedLicense": []}""", "Unknown license policy field"),
        Arguments.of("""{"allowedLicenses": "MIT"}""", "allowedLicenses must be an array"),
        Arguments.of("""{"allowedLicenses": [" "]}""", "nonempty SPDX identifiers"),
        Arguments.of(
          """{"allowedUrls": [{"url":"https://fixture.invalid"}]}""",
          "allowedUrls entries must contain exactly",
        ),
        Arguments.of(
          """{"allowedUrls": [{"url":"https://fixture.invalid","reason":""}]}""",
          "allowedUrls.reason must be a nonempty string",
        ),
        Arguments.of(
          """{"allowedUrls": [{"url":"x","reason":"ok","extra":true}]}""",
          "allowedUrls entries must contain exactly",
        ),
        Arguments.of(
          """{"allowedDependencies": [{"group":"example","artifact":"a","version":"","reason":"ok"}]}""",
          "allowedDependencies.version must be a nonempty string",
        ),
        Arguments.of(
          """{"allowedDependencies": [{"group":"example","artifact":"a","version":"1"}]}""",
          "allowedDependencies entries must contain exactly",
        ),
        Arguments.of("""{"allowedUrls": [7]}""", "allowedUrls entries must contain exactly"),
        Arguments.of("""{"_comment": true}""", "_comment must be a string or array of strings"),
      )
  }
}

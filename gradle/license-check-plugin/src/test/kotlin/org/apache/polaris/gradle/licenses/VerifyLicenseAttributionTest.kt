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

import java.nio.file.Path
import kotlin.io.path.readText
import kotlin.io.path.writeText
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.gradle.api.GradleException
import org.gradle.testfixtures.ProjectBuilder
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class VerifyLicenseAttributionTest {
  @TempDir lateinit var directory: Path

  @Test
  fun verifiesExternalCoverageAndKnownGeneratedFiles() {
    val task = fixture()
    task.verify()
    assertThat(report()).contains("External shipped Maven coordinates: 1", "generated-bytecode.jar")
  }

  @ParameterizedTest
  @ValueSource(
    strings =
      [
        "{\"components\":[]}",
        "{\"components\":[{\"name\":\"unknown.jar\",\"purl\":\"pkg:generic/unknown.jar@1\"}]}",
        "{\"components\":[{\"name\":\"generated-bytecode.jar\",\"purl\":\"pkg:generic/unrelated@1\"}]}",
        "{\"components\":[{\"group\":\"example\",\"name\":\"library\",\"version\":\" \",\"purl\":\"pkg:maven/example/library@1\"}]}",
        "{\"components\":[{\"group\":\"org.apache.polaris\",\"name\":\"server\",\"version\":\"1\",\"purl\":\"pkg:maven/org.apache.polaris/server@1\"}]}",
      ],
  )
  fun rejectsInvalidOrEmptyInventory(inventory: String) {
    val task = fixture()
    directory.resolve("sbom.json").writeText(inventory)
    assertThatThrownBy(task::verify).isInstanceOf(IllegalArgumentException::class.java)
  }

  @Test
  fun requiresNonemptyPolicyEvidence() {
    val task = fixture()
    directory.resolve("artifacts.json").writeText("[]")
    assertThatThrownBy(task::verify).hasMessageContaining("nonempty policy evidence")
  }

  @Test
  fun requiresPolicyEvidenceForTheShippedVersion() {
    val task = fixture()
    directory
      .resolve("artifacts.json")
      .writeText("""[{"groupId":"example","artifactId":"library","version":"2"}]""")
    assertThatThrownBy(task::verify).isInstanceOf(GradleException::class.java)
    assertThat(report())
      .contains("Shipped components not checked by Licensee (1):\n- example:library:1")
  }

  @Test
  fun requiresExactLicenseMarker() {
    val task = fixture()
    directory.resolve("LICENSE").writeText("* Maven group:artifact IDs: prefix-example:library\n")
    assertThatThrownBy(task::verify).isInstanceOf(GradleException::class.java)
    assertThat(report())
      .contains("Shipped components missing LICENSE markers (1):\n- example:library")
  }

  @Test
  fun rejectsWhitespaceNotice() {
    val task = fixture()
    directory.resolve("NOTICE").writeText(" \n")
    assertThatThrownBy(task::verify).isInstanceOf(GradleException::class.java)
    assertThat(report()).contains("NOTICE is non-empty: false")
  }

  private fun fixture(): VerifyLicenseAttribution {
    val project = ProjectBuilder.builder().withProjectDir(directory.toFile()).build()
    val task =
      project.tasks.register("verifyAttribution", VerifyLicenseAttribution::class.java).get()
    task.licenseeReport.set(directory.resolve("artifacts.json").toFile())
    task.sbom.set(directory.resolve("sbom.json").toFile())
    task.licenseFile.set(directory.resolve("LICENSE").toFile())
    task.noticeFile.set(directory.resolve("NOTICE").toFile())
    task.validationReport.set(directory.resolve("validation.txt").toFile())
    task.projectGroup.set("org.apache.polaris")
    task.generatedComponentNames.set(setOf("generated-bytecode.jar"))
    directory
      .resolve("artifacts.json")
      .writeText("""[{"groupId":"example","artifactId":"library","version":"1"}]""")
    directory
      .resolve("sbom.json")
      .writeText(
        """{"components":[
      {"group":"example","name":"library","version":"1","purl":"pkg:maven/example/library@1"},
      {"group":"org.apache.polaris","name":"server","version":"1","purl":"pkg:maven/org.apache.polaris/server@1"},
      {"name":"generated-bytecode.jar","purl":"pkg:generic/generated-bytecode.jar@1"},
      {"name":"excluded.jar","scope":"excluded"}
    ]}""",
      )
    directory.resolve("LICENSE").writeText("* Maven group:artifact IDs: example:library\n")
    directory.resolve("NOTICE").writeText("Fixture notice\n")
    return task
  }

  private fun report() = directory.resolve("validation.txt").readText()
}

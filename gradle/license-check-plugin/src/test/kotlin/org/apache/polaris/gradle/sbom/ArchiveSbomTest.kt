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

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.cyclonedx.model.Bom
import org.cyclonedx.model.Component
import org.cyclonedx.model.Hash
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class ArchiveSbomTest {
  private val hash = "a".repeat(64)
  private val files =
    mapOf(
      "polaris-bin-1/admin/quarkus-run.jar" to hash,
      "polaris-bin-1/server/quarkus-run.jar" to hash,
      "polaris-bin-1/LICENSE" to "b".repeat(64),
    )

  private fun producer(): Bom =
    SbomCore.readBom(
      """
    {"bomFormat":"CycloneDX","specVersion":"1.6","version":1,
     "metadata":{"component":{"type":"application","name":"launcher","bom-ref":"launcher",
       "hashes":[{"alg":"SHA-256","content":"$hash"}],
       "evidence":{"occurrences":[{"location":"quarkus-run.jar"}]}}}}
    """
        .reader(),
    )

  private fun finalize(
    files: Map<String, String> = this.files,
    admin: Bom = producer(),
    kind: PackageKind = PackageKind.BINARY_ZIP,
  ): Bom =
    ArchiveSbom.finalize(
      SbomSubject("org.apache.polaris", "polaris-bin", "1", kind),
      "polaris-bin-1.zip",
      "c".repeat(64),
      files,
      mapOf("admin" to admin, "server" to producer()),
    )

  @ParameterizedTest
  @EnumSource(value = PackageKind::class, names = ["BINARY_ZIP", "BINARY_TAR"])
  fun identifiesArchiveBytesAndKeepsApplicationOccurrencesSeparate(kind: PackageKind) {
    val result = finalize(kind = kind)
    val root = result.metadata.component
    assertThat(root.hashes.single().value).isEqualTo("c".repeat(64))
    assertThat(root.purl).contains("variant=${kind.type}")
    assertThat(root.components.map { it.name })
      .containsExactly("polaris-admin", "polaris-server", "polaris-bin-1/LICENSE")
    assertThat(root.components.take(2).map { it.hashes }).containsOnlyNulls()
    assertThat(
        root.components
          .take(2)
          .flatMap { it.components }
          .map { it.evidence.occurrences.single().location },
      )
      .containsExactly(
        "polaris-bin-1/admin/quarkus-run.jar",
        "polaris-bin-1/server/quarkus-run.jar",
      )
    SbomCore.checkBomReferences(result)
  }

  @Test
  fun retainsProducerReportedBundledMetadataWithinItsPackagedParent() {
    val admin =
      producer().apply {
        metadata.component.components =
          listOf(
            Component().apply {
              type = Component.Type.LIBRARY
              name = "inside-launcher"
              bomRef = "nested"
              hashes = listOf(Hash(Hash.Algorithm.SHA_256, "d".repeat(64)))
            },
          )
      }
    val launcher = finalize(admin = admin).metadata.component.components.first().components.single()
    assertThat(launcher.components.single().name).isEqualTo("inside-launcher")
    assertThat(launcher.components.single().hashes).isNull()
    assertThat(launcher.components.single().properties.single().value)
      .contains("not a separately shipped file")
  }

  @Test
  fun rejectsChangedPackagedBytes() {
    assertThatThrownBy {
        finalize(files + ("polaris-bin-1/admin/quarkus-run.jar" to "d".repeat(64)))
      }
      .hasMessageContaining("Packaged bytes differ")
  }

  @Test
  fun rejectsMissingPackagedInput() {
    assertThatThrownBy { finalize(files - "polaris-bin-1/server/quarkus-run.jar") }
      .hasMessageContaining("Packaged bytes differ or are missing")
  }

  @Test
  fun rejectsMissingProducerEvidence() {
    val admin = producer().apply { metadata.component.evidence = null }
    assertThatThrownBy { finalize(admin = admin) }.hasMessageContaining("Missing packaged location")
  }

  @Test
  fun rejectsConflictingProducerReferences() {
    val admin =
      producer().apply {
        components = listOf(producer().metadata.component.apply { name = "conflicting" })
      }
    assertThatThrownBy { finalize(admin = admin) }
      .hasMessageContaining("Conflicting component reference")
  }
}

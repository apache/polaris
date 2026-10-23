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
import java.io.File
import java.time.Instant
import java.util.Date
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.cyclonedx.model.Bom
import org.cyclonedx.model.Component
import org.cyclonedx.model.Composition
import org.cyclonedx.model.Dependency
import org.cyclonedx.model.Hash
import org.cyclonedx.model.Metadata
import org.cyclonedx.model.Property
import org.cyclonedx.model.metadata.ToolInformation
import org.cyclonedx.parsers.JsonParser
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class SbomCoreTest {
  @TempDir lateinit var dir: File

  @Test
  fun sparkBundleSubjectIsDistinctFromThinJar() {
    val thin =
      SbomSubject("org.apache.polaris", "polaris-spark-3.5_2.12", "1", PackageKind.SHADED_JAR)
    val bundle =
      SbomSubject(
        "org.apache.polaris",
        "polaris-spark-3.5_2.12",
        "1",
        PackageKind.SHADED_JAR,
        "bundle",
      )
    assertThat(bundle.purl).isNotEqualTo(thin.purl).contains("classifier=bundle")
    assertThat(bundle.sbomFilename).isEqualTo("polaris-spark-3.5_2.12-1-bundle.jar.cdx.json")
    assertThat(bundle.sbomClassifier).isEqualTo("bundle-sbom-cyclonedx")
  }

  private fun component(name: String, classifier: String = ""): Component =
    Component().apply {
      type = Component.Type.LIBRARY
      group = "example"
      this.name = name
      version = "1"
      setPurl(
        aPackageURL()
          .withType("maven")
          .withNamespace(group)
          .withName(name)
          .withVersion(version)
          .withQualifier("type", "jar")
          .apply { if (classifier.isNotEmpty()) withQualifier("classifier", classifier) }
          .build(),
      )
      bomRef = purl
    }

  private fun producer(): Bom {
    val root = component("application")
    val a = component("first-party")
    val b = component("opaque-runtime")
    val provided = component("provided")
    return Bom().apply {
      metadata = Metadata().apply { component = root }
      components = listOf(a, b, provided)
      dependencies =
        listOf(
          Dependency(root.bomRef).apply {
            dependencies = listOf(Dependency(a.bomRef), Dependency(provided.bomRef))
          },
          Dependency(a.bomRef).apply { dependencies = listOf(Dependency(b.bomRef)) },
        )
    }
  }

  private fun input(name: String, classifier: String = "", hash: String = "a".repeat(64)) =
    SelectedInput(component(name, classifier), hash)

  @Test
  fun selectedProjectionPreservesMixedInputsAndSupportedEdges() {
    val timestamp = Instant.parse("2026-10-23T12:00:00Z")
    val source =
      producer().apply {
        serialNumber = "urn:uuid:12345678-1234-1234-1234-123456789abc"
        metadata.timestamp = Date.from(timestamp)
        metadata.component.description = "Keep producer description"
        metadata.toolChoice =
          ToolInformation().apply { components = listOf(component("native-producer")) }
      }
    val result =
      SbomCore.forDistribution(
        source,
        SbomSubject("example", "bundle", "1", PackageKind.SHADED_JAR),
        listOf(input("first-party"), input("opaque-runtime")),
        "b".repeat(64),
      )
    val bom = JsonParser().parse(SbomCore.bomToJson(result).reader())
    assertThat(bom.serialNumber).isEqualTo("urn:uuid:12345678-1234-1234-1234-123456789abc")
    assertThat(bom.metadata.timestamp.toInstant()).isEqualTo(timestamp)
    assertThat(bom.metadata.component.description).isEqualTo("Keep producer description")
    assertThat(bom.metadata.toolChoice.components.single().name).isEqualTo("native-producer")
    assertThat(SbomCore.components(bom).map { it.name })
      .containsExactly("bundle", "first-party", "opaque-runtime")
    assertThat(bom.dependencies).hasSize(1)
    assertThat(bom.compositions.single().aggregate).isEqualTo(Composition.Aggregate.UNKNOWN)
  }

  @Test
  fun firstPartyOnlyDoesNotAdvertiseProvidedOrUnselectedInputs() {
    val bom =
      SbomCore.forDistribution(
        producer(),
        SbomSubject("example", "bundle", "1", PackageKind.SHADED_JAR),
        listOf(input("first-party")),
        "b".repeat(64),
      )
    assertThat(SbomCore.components(bom).map { it.name }).containsExactly("bundle", "first-party")
  }

  @Test
  fun classifierMismatchAndInvalidGraphFailClosed() {
    assertThatThrownBy {
        SbomCore.forDistribution(
          producer(),
          SbomSubject("example", "bundle", "1", PackageKind.SHADED_JAR),
          listOf(input("absent", "tests")),
          "b".repeat(64),
        )
      }
      .hasMessageContaining("missing or ambiguous")
    val source = producer().apply { dependencies = listOf(Dependency("missing")) }
    assertThatThrownBy { SbomCore.checkBomReferences(source) }.hasMessageContaining("Dangling")
    val invalid =
      File(dir, "invalid.json").apply {
        writeText("""{"bomFormat":"CycloneDX","specVersion":"1.6","version":"bad"}""")
      }
    assertThatThrownBy { SbomCore.readBom(invalid) }.hasMessageContaining("schema")
  }

  @Test
  fun classifierProjectionKeepsDistinctInputsWithoutInventingArtifactEdges() {
    val selected =
      listOf(
        input("first-party", "linux", "c".repeat(64)),
        input("first-party", "macos", "c".repeat(64)),
      )
    val bom =
      SbomCore.forDistribution(
        producer(),
        SbomSubject("example", "bundle", "1", PackageKind.SHADED_JAR),
        selected,
        "b".repeat(64),
      )
    assertThat(SbomCore.components(bom).map { it.purl })
      .contains(selected[0].component.purl, selected[1].component.purl)
    assertThat(bom.dependencies).isEmpty()
  }

  @Test
  fun reusesSingleUseComponentsAndCopiesSharedMetadata() {
    for (classifiedFirst in listOf(false, true)) {
      val source = producer()
      val shared =
        source.components[0].apply {
          hashes = listOf(Hash(Hash.Algorithm.SHA_256, "a".repeat(64)))
          addProperty(Property("fixture-marker", "retained"))
        }
      val singleUse = source.components[1]
      val base = input("first-party")
      val classified = input("first-party", "linux", "c".repeat(64))
      val variants = if (classifiedFirst) listOf(classified, base) else listOf(base, classified)
      val bom =
        SbomCore.forDistribution(
          source,
          SbomSubject("example", "bundle", "1", PackageKind.SHADED_JAR),
          variants + input("opaque-runtime"),
          "b".repeat(64),
        )
      val components = SbomCore.components(bom).associateBy { it.purl }
      val baseOutput = components.getValue(base.component.purl)
      val classifiedOutput = components.getValue(classified.component.purl)
      assertThat(components.getValue(singleUse.purl)).isSameAs(singleUse)
      assertThat(baseOutput).isNotSameAs(shared).isNotSameAs(classifiedOutput)
      assertThat(classifiedOutput).isNotSameAs(shared)
      assertThat(shared.properties.map { it.name }).containsExactly("fixture-marker")
      assertThat(shared.hashes.single().value).isEqualTo("a".repeat(64))
      for ((output, expected) in listOf(baseOutput to base, classifiedOutput to classified)) {
        assertThat(
            output.properties.filter { it.name == "distribution:input:sha256" }.map { it.value },
          )
          .containsExactly(expected.sha256)
      }
      assertThat(bom.dependencies.single().ref).isEqualTo(base.component.purl)
      assertThat(bom.dependencies.single().dependencies.single().ref).isEqualTo(singleUse.bomRef)
    }
  }
}

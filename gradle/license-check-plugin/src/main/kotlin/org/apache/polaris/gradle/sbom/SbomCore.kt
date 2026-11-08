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

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.github.packageurl.PackageURL
import com.github.packageurl.PackageURLBuilder.aPackageURL
import java.io.File
import java.io.Reader
import org.cyclonedx.Version
import org.cyclonedx.generators.BomGeneratorFactory
import org.cyclonedx.model.Bom
import org.cyclonedx.model.BomReference
import org.cyclonedx.model.Component
import org.cyclonedx.model.Composition
import org.cyclonedx.model.Dependency
import org.cyclonedx.model.ExternalReference
import org.cyclonedx.model.Hash
import org.cyclonedx.model.Metadata
import org.cyclonedx.model.Property
import org.cyclonedx.parsers.JsonParser
import org.cyclonedx.util.BomUtils

/** Original artifact bytes selected by the assembly, with their resolved Maven identity. */
data class SelectedInput(val component: Component, val sha256: String)

/** Assembly-specific transformations; CycloneDX owns the BOM model, schema and serialization. */
object SbomCore {
  internal val mapper: JsonMapper =
    JsonMapper.builder().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS).build()

  /** CycloneDX version used here. Keep in sync with the CycloneDX version that Quarkus uses. */
  val cycloneDxVersion = Version.VERSION_16

  internal fun readBom(file: File): Bom =
    file.reader().use {
      readBom(it)
    }

  internal fun readBom(reader: Reader): Bom {
    val jsonNode = mapper.readTree(reader)
    val errors = JsonParser().validate(jsonNode, cycloneDxVersion)
    require(errors.isEmpty()) {
      "CycloneDX schema validation: ${errors.joinToString { it.message.orEmpty() }}"
    }
    return mapper.treeToValue(jsonNode, Bom::class.java)
  }

  internal fun toJson(value: Any): String =
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(value) + "\n"

  internal fun bomToJson(bom: Bom): String =
    BomGeneratorFactory.createJson(cycloneDxVersion, bom).toJsonString() + "\n"

  internal fun writeBomFile(file: File, bom: Bom) {
    file.parentFile.mkdirs()
    file.writeText(bomToJson(bom))
  }

  /** Discover mutable assessments without fetching or pinning a particular VEX revision. */
  internal fun addVexReference(bom: Bom, url: String) {
    val type = ExternalReference.Type.EXPLOITABILITY_STATEMENT
    val references = bom.externalReferences.orEmpty()
    if (references.none { it.type == type && it.url == url }) {
      bom.externalReferences =
        references +
          ExternalReference().apply {
            this.type = type
            this.url = url
          }
    }
  }

  internal fun hash(file: File): String =
    requireNotNull(
        BomUtils.calculateHashes(file, cycloneDxVersion, listOf(Hash.Algorithm.SHA_256)),
      ) {
        "Cannot hash artifact: $file"
      }
      .single()
      .value

  /** Project-path qualifiers are producer references, not published artifact classifiers. */
  private fun identity(component: Component): String {
    val qualifiers =
      PackageURL(requireNotNull(component.purl) { "Selected Maven input lacks purl" })
        .qualifiers
        .orEmpty()
    return aPackageURL()
      .withType("maven")
      .withNamespace(component.group)
      .withName(component.name)
      .withVersion(component.version)
      .withQualifier("type", qualifiers["type"] ?: "jar")
      .apply {
        qualifiers["classifier"]
          ?.takeIf { it.isNotEmpty() }
          ?.let { withQualifier("classifier", it) }
      }
      .build()
      .canonicalize()
  }

  internal fun components(bom: Bom): List<Component> {
    fun children(component: Component): List<Component> =
      component.components.orEmpty().flatMap { listOf(it) + children(it) }
    val root = bom.metadata.component
    return listOf(root) +
      children(root) +
      bom.components.orEmpty().flatMap { listOf(it) + children(it) }
  }

  internal fun checkBomReferences(bom: Bom) {
    val values = components(bom).map { requireNotNull(it.bomRef) { "Missing component reference" } }
    require(values.size == values.toSet().size) { "Duplicate component references" }
    val refs = values.toSet()
    val edges = bom.dependencies.orEmpty()
    require(edges.map { it.ref }.distinct().size == edges.size) {
      "Duplicate dependency references"
    }
    edges.forEach { edge ->
      require(edge.ref in refs) { "Dangling dependency source: ${edge.ref}" }
      edge.dependencies.orEmpty().forEach {
        require(it.ref in refs) { "Dangling dependency target: ${it.ref}" }
      }
    }
  }

  private fun componentJson(component: Component): String =
    bomToJson(Bom().apply { metadata = Metadata().apply { this.component = component } })

  private fun copyComponent(component: Component): Component =
    JsonParser().parse(componentJson(component).reader()).metadata.component

  /** Updates the bom in place, retaining only supported relationships between assembly inputs. */
  internal fun forDistribution(
    bom: Bom,
    subject: SbomSubject,
    selected: List<SelectedInput>,
    digest: String,
  ): Bom {
    val root = bom.metadata.component
    checkBomReferences(bom)
    val oldRootRef = root.bomRef
    val source =
      components(bom).filter { it.bomRef != oldRootRef && it.scope != Component.Scope.EXCLUDED }

    val remapped = mutableMapOf<String, String>()

    require(selected.isNotEmpty()) { "Empty selected dependency inventory" }
    val byIdentity = source.filter { it.purl?.startsWith("pkg:maven/") == true }.groupBy(::identity)
    val selectedIds = selected.map { identity(it.component) }
    require(selectedIds.distinct().size == selectedIds.size) {
      "Ambiguous selected input identity"
    }
    val resolved =
      selected.zip(selectedIds).map { (input, id) ->
        val matches =
          byIdentity[id].orEmpty().ifEmpty {
            source.filter {
              it.group == input.component.group &&
                it.name == input.component.name &&
                it.version == input.component.version
            }
          }
        require(matches.size == 1) { "Selected input missing or ambiguous in producer: $id" }
        input to matches.single()
      }
    val usesByRef = resolved.groupingBy { it.second.bomRef }.eachCount()
    val chosen = resolved.map { (input, component) ->
      val id = identity(input.component)
      require(input.sha256.matches(Regex("[a-f0-9]{64}"))) { "Missing input SHA-256" }
      val hashes = component.hashes.orEmpty().filter { it.algorithm == Hash.Algorithm.SHA_256.spec }
      val artifactMatch =
        if (hashes.isEmpty()) identity(component) == id else hashes.any { it.value == input.sha256 }
      if (artifactMatch) {
        require(component.bomRef !in remapped) {
          "Ambiguous artifact graph reference: ${component.bomRef}"
        }
        remapped[component.bomRef] = id
      }
      // Only shared module metadata needs separate copies for its classifier inputs.
      val projected =
        if (usesByRef.getValue(component.bomRef) > 1) copyComponent(component) else component
      projected.apply {
        bomRef = id
        purl = id
        this.hashes = null
        components = null
        addProperty(Property("distribution:input:sha256", input.sha256))
      }
    }

    root.apply {
      subject.applyToComponent(this)
      type = Component.Type.APPLICATION
      scope = Component.Scope.REQUIRED
      hashes = listOf(Hash(Hash.Algorithm.SHA_256, digest))
      components = chosen.sortedBy { it.bomRef }
      evidence = null
      modified = null
    }

    bom.components = null
    bom.compositions =
      listOf(
        Composition().apply {
          aggregate = Composition.Aggregate.UNKNOWN
          assemblies = listOf(BomReference(root.bomRef))
        },
      )
    bom.dependencies =
      bom.dependencies.orEmpty().mapNotNull { edge ->
        val ref = remapped[edge.ref]
        if (ref == null) return@mapNotNull null
        // Missing relationships remain unknown. Only an explicit empty selected-input row proves a
        // leaf.
        val original = edge.dependencies ?: return@mapNotNull null
        val targets = original.mapNotNull { remapped[it.ref]?.let(::Dependency) }
        if (targets.isEmpty() && original.isNotEmpty()) null
        else Dependency(ref).apply { dependencies = targets }
      }

    // validate the generated bom via readBom()
    readBom(bomToJson(bom).reader())
    checkBomReferences(bom)

    return bom
  }
}

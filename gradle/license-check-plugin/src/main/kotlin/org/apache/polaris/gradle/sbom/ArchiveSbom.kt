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

import com.fasterxml.jackson.databind.JsonNode
import org.cyclonedx.model.Bom
import org.cyclonedx.model.BomReference
import org.cyclonedx.model.Component
import org.cyclonedx.model.Composition
import org.cyclonedx.model.Dependency
import org.cyclonedx.model.Hash
import org.cyclonedx.model.Metadata
import org.cyclonedx.model.Property

/** Polaris binary archives contain two fast-JAR applications plus scripts and attribution. */
internal object ArchiveSbom {
  fun finalize(
    subject: SbomSubject,
    filename: String,
    digest: String,
    files: Map<String, String>,
    applications: Map<String, Bom>,
  ): Bom {
    require(files.isNotEmpty()) { "Empty binary archive" }
    val roots = files.keys.map { it.substringBefore('/') }.distinct()
    require(roots.size == 1 && files.keys.all { '/' in it }) { "Expected one archive root" }
    val prefix = roots.single()
    val claimed = mutableSetOf<String>()
    val dependencies = mutableListOf<Dependency>()
    val children =
      applications
        .toSortedMap()
        .map { (application, bom) ->
          val root = bom.metadata.component
          val appRef = "urn:polaris:archive:$application"
          val parents = mutableMapOf<String, String>()
          fun recordParents(component: Component) {
            component.components.orEmpty().forEach { child ->
              require(
                parents.put(child.bomRef, component.bomRef).let {
                  it == null || it == component.bomRef
                },
              ) {
                "Ambiguous bundled parent: ${child.bomRef}"
              }
              recordParents(child)
            }
          }
          (listOf(root) + bom.components.orEmpty()).forEach(::recordParents)
          val all =
            SbomCore.components(bom)
              .filter { it.scope != Component.Scope.EXCLUDED }
              .groupBy { requireNotNull(it.bomRef) { "Missing component reference" } }
              .map { (ref, entries) ->
                require(
                  entries.map { SbomCore.mapper.valueToTree<JsonNode>(it) }.distinct().size == 1,
                ) {
                  "Conflicting component reference: $ref"
                }
                entries.first()
              }
          val byRef = all.associateBy { it.bomRef }
          fun packaged(component: Component): Boolean {
            val locations = component.evidence?.occurrences.orEmpty().map { it.location }
            if (locations == listOf("quarkus-app-dependencies.txt")) return false
            if (locations.isEmpty()) {
              val parent = parents[component.bomRef]?.let { byRef[it] }
              require(parent != null) { "Missing packaged location: ${component.bomRef}" }
              return packaged(parent)
            }
            val hash = component.hashes.orEmpty().singleOrNull { it.algorithm == "SHA-256" }?.value
            require(hash != null) { "Missing packaged SHA-256: ${component.bomRef}" }
            locations.forEach { location ->
              val path = "$prefix/$application/$location"
              require(files[path] == hash) { "Packaged bytes differ or are missing: $path" }
              claimed += path
            }
            return true
          }
          val kept = all.filter(::packaged)
          val top = kept.filter { it.bomRef !in parents }
          val refs = kept.associate { it.bomRef to "$appRef:${it.bomRef}" }
          bom.dependencies.orEmpty().forEach { edge ->
            val ref = if (edge.ref == root.bomRef) appRef else refs[edge.ref]
            if (ref != null && edge.dependencies != null) {
              val targets = edge.dependencies.mapNotNull { refs[it.ref]?.let(::Dependency) }
              if (targets.isNotEmpty())
                dependencies += Dependency(ref).apply { this.dependencies = targets }
            }
          }
          Component().apply {
            type = Component.Type.APPLICATION
            name = "polaris-$application"
            version = subject.version
            bomRef = appRef
            kept.forEach { component ->
              component.evidence?.occurrences.orEmpty().forEach { occurrence ->
                occurrence.location = "$prefix/$application/${occurrence.location}"
              }
              if (component.bomRef in parents && component.evidence?.occurrences.isNullOrEmpty()) {
                component.hashes = null
                component.addProperty(
                  Property(
                    "distribution:input:role",
                    "producer-reported bundled metadata; not a separately shipped file",
                  ),
                )
              }
              component.components = component.components?.filter { it.bomRef in refs }
              component.bomRef = refs.getValue(component.bomRef)
              if (component === root) component.type = Component.Type.FILE
            }
            components = top.sortedBy { it.bomRef }
          }
        }
        .toMutableList()
    files
      .toSortedMap()
      .filterKeys { it !in claimed }
      .forEach { (path, hash) ->
        children +=
          Component().apply {
            type = Component.Type.FILE
            name = path
            bomRef = "urn:polaris:archive:file:$path"
            hashes = listOf(Hash(Hash.Algorithm.SHA_256, hash))
          }
      }
    val root =
      Component().apply {
        subject.applyToComponent(this)
        type = Component.Type.APPLICATION
        hashes = listOf(Hash(Hash.Algorithm.SHA_256, digest))
        components = children
        addProperty(Property("distribution:subject:filename", filename))
        addProperty(Property("distribution:subject:digest-kind", "final archive SHA-256"))
      }
    return Bom().apply {
      metadata = Metadata().apply { component = root }
      this.dependencies = dependencies
      compositions =
        listOf(
          Composition().apply {
            aggregate = Composition.Aggregate.UNKNOWN
            assemblies = listOf(BomReference(root.bomRef))
          },
        )
      SbomCore.readBom(SbomCore.bomToJson(this).reader())
      SbomCore.checkBomReferences(this)
    }
  }
}

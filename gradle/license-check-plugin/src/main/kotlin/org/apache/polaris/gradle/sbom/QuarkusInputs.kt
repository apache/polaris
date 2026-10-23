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

import java.io.File
import org.cyclonedx.model.Bom
import org.cyclonedx.model.Component
import org.cyclonedx.model.Hash
import org.cyclonedx.model.Property

/** Bind native producer locations to actual fast-JAR bytes, keeping producer hashes distinct. */
internal object QuarkusInputs {
  fun bind(bom: Bom, directory: File) {
    val root = directory.toPath().toRealPath()
    SbomCore.components(bom)
      .filter { it.scope != Component.Scope.EXCLUDED }
      .forEach { component ->
        val locations = component.evidence?.occurrences.orEmpty().map { it.location }
        if (locations.isNotEmpty()) {
          val hashes =
            locations
              .map { location ->
                val path = root.resolve(location).normalize()
                require(path.startsWith(root) && path.toRealPath().startsWith(root)) {
                  "Invalid producer location: $location"
                }
                SbomCore.hash(path.toFile())
              }
              .distinct()
          require(hashes.size == 1) {
            "Different bytes for producer occurrences: ${component.bomRef}"
          }
          if (
            component.hashes.orEmpty().any {
              it.algorithm == "SHA-256" && it.value != hashes.single()
            }
          ) {
            component.addProperty(
              Property(
                "distribution:producer:hashes",
                SbomCore.toJson(
                    component.hashes.map { mapOf("alg" to it.algorithm, "content" to it.value) },
                  )
                  .trim(),
              ),
            )
          }
          component.hashes = listOf(Hash(Hash.Algorithm.SHA_256, hashes.single()))
        }
      }
  }
}

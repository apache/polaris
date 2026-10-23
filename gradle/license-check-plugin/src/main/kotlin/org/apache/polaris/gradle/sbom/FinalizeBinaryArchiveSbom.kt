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

import javax.inject.Inject
import org.gradle.api.DefaultTask
import org.gradle.api.file.ArchiveOperations
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.Nested
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction

@CacheableTask
abstract class FinalizeBinaryArchiveSbom
@Inject
constructor(private val archives: ArchiveOperations) : DefaultTask() {
  @get:InputFile
  @get:PathSensitive(PathSensitivity.NAME_ONLY)
  abstract val archive: RegularFileProperty
  @get:InputFile
  @get:PathSensitive(PathSensitivity.NONE)
  abstract val adminProducer: RegularFileProperty
  @get:InputFile
  @get:PathSensitive(PathSensitivity.NONE)
  abstract val serverProducer: RegularFileProperty
  @get:InputDirectory
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val adminTree: DirectoryProperty
  @get:InputDirectory
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val serverTree: DirectoryProperty
  @get:Nested abstract val subject: Property<SbomSubject>
  @get:OutputFile abstract val document: RegularFileProperty

  @TaskAction
  fun finalizeDocument() {
    val file = archive.get().asFile
    val tree =
      if (subject.get().packageKind == PackageKind.BINARY_ZIP) archives.zipTree(file)
      else archives.tarTree(file)
    val files = sortedMapOf<String, String>()
    tree.visit {
      if (!isDirectory) {
        val name = relativePath.pathString
        require(files.put(name, SbomCore.hash(this.file)) == null) {
          "Duplicate archive entry: $name"
        }
      }
    }
    val result =
      ArchiveSbom.finalize(
        subject.get(),
        file.name,
        SbomCore.hash(file),
        files,
        mapOf(
          "admin" to
            SbomCore.readBom(adminProducer.get().asFile).also {
              QuarkusInputs.bind(it, adminTree.get().asFile)
            },
          "server" to
            SbomCore.readBom(serverProducer.get().asFile).also {
              QuarkusInputs.bind(it, serverTree.get().asFile)
            },
        ),
      )
    SbomCore.writeBomFile(document.get().asFile, result)
  }
}

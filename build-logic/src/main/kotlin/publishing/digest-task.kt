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

package publishing

import java.io.File
import java.security.MessageDigest
import javax.inject.Inject
import org.gradle.api.DefaultTask
import org.gradle.api.Project
import org.gradle.api.model.ObjectFactory
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.OutputFiles
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.TaskProvider
import org.gradle.internal.extensions.stdlib.capitalized
import org.gradle.work.DisableCachingByDefault

@DisableCachingByDefault
abstract class GenerateDigest @Inject constructor(objectFactory: ObjectFactory) : DefaultTask() {

  @get:InputFiles val files = objectFactory.fileCollection()

  @get:Input val algorithm = objectFactory.property(String::class.java).convention("SHA-512")

  @Suppress("unused", "UnstableApiUsage")
  @get:OutputFiles
  val outputFiles =
    objectFactory.fileCollection().convention(files.map { file -> digestFileForInput(file) })

  @TaskAction
  fun generate() {
    files.files.forEach { input -> digest(input) }
  }

  private fun digestFileForInput(input: File): File {
    val algo = algorithm.get()
    return input.parentFile.resolve("${input.name}.${algo.replace("-", "").lowercase()}")
  }

  private fun digest(input: File) {
    val algo = algorithm.get()
    logger.info("Generating {} digest for '{}'", algo, input)
    val digestFile = digestFileForInput(input)
    val md = MessageDigest.getInstance(algo)
    input.inputStream().use {
      val buf = ByteArray(8192)
      var rd: Int
      while (true) {
        rd = it.read(buf)
        if (rd == -1) break
        md.update(buf, 0, rd)
      }

      digestFile.writeText(
        md.digest().joinToString(separator = "") { eachByte -> "%02x".format(eachByte) } +
          "  ${input.name}"
      )
    }
  }
}

fun Project.digestTaskOutputs(task: TaskProvider<*>): TaskProvider<GenerateDigest> {
  val digestTask = tasks.register("digest${task.name.capitalized()}", GenerateDigest::class.java)
  digestTask.configure {
    dependsOn(task)
    this.files.from(task.map { t -> t.outputs.files }.get())
  }
  task.configure { finalizedBy(digestTask) }
  return digestTask
}

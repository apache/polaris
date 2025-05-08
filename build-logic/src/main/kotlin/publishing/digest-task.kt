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

import java.security.MessageDigest
import javax.inject.Inject
import org.gradle.api.DefaultTask
import org.gradle.api.model.ObjectFactory
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction
import org.gradle.work.DisableCachingByDefault

@DisableCachingByDefault
abstract class GenerateDigest @Inject constructor(objectFactory: ObjectFactory) : DefaultTask() {

  @get:InputFile val file = objectFactory.fileProperty()
  @get:Input val algorithm = objectFactory.property(String::class.java).convention("SHA-512")
  @get:OutputFile
  val outputFile =
    objectFactory.fileProperty().convention {
      val input = file.get().asFile
      val algo = algorithm.get()
      input.parentFile.resolve("${input.name}.${algo.replace("-", "").lowercase()}")
    }

  @TaskAction
  fun generate() {
    val input = file.get().asFile
    val digestFile = outputFile.get().asFile
    val md = MessageDigest.getInstance(algorithm.get())
    input.inputStream().use {
      val buffered = it.buffered(8192)
      val buf = ByteArray(8192)
      var rd: Int
      while (true) {
        rd = buffered.read(buf)
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

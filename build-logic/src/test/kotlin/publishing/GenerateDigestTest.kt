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

import java.nio.file.Path
import java.security.MessageDigest
import org.assertj.core.api.Assertions.assertThat
import org.gradle.testfixtures.ProjectBuilder
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class GenerateDigestTest {
  @Test
  fun declaredOutputsFollowLateInputsAndAlgorithmChanges(@TempDir dir: Path) {
    val project = ProjectBuilder.builder().withProjectDir(dir.toFile()).build()
    val task = project.tasks.register("digest", GenerateDigest::class.java).get()
    assertThat(task.outputFiles.files).isEmpty()
    val input = dir.resolve("subject.json").toFile().apply { writeText("subject") }
    task.files.from(input)
    val sha512 = dir.resolve("subject.json.sha512").toFile()
    assertThat(task.outputFiles.files).containsExactly(sha512)
    task.generate()
    val expected =
      MessageDigest.getInstance("SHA-512").digest(input.readBytes()).joinToString("") {
        "%02x".format(it)
      }
    assertThat(sha512.readText()).isEqualTo("$expected  subject.json")

    task.algorithm.set("SHA-256")
    val replacement = dir.resolve("replacement.json").toFile().apply { writeText("replacement") }
    task.files.setFrom(replacement)
    assertThat(task.outputFiles.files)
      .containsExactly(dir.resolve("replacement.json.sha256").toFile())
    task.generate()
    assertThat(task.outputFiles.files.single()).exists()
  }
}

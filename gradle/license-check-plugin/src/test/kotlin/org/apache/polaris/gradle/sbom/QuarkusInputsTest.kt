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
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class QuarkusInputsTest {
  @TempDir lateinit var directory: File

  private fun producer(location: String) =
    SbomCore.readBom(
      """
    {"bomFormat":"CycloneDX","specVersion":"1.6","version":1,
      "metadata":{"component":{"type":"application","name":"launcher","bom-ref":"root",
      "hashes":[{"alg":"SHA-256","content":"${"a".repeat(64)}"}],
      "evidence":{"occurrences":[{"location":"$location"}]}}}}
    """
        .reader(),
    )

  @Test
  fun bindsActualBytesAndPreservesDifferentProducerHashes() {
    val launcher = File(directory, "launcher.jar").apply { writeText("actual assembled bytes") }
    val bom = producer("launcher.jar")
    QuarkusInputs.bind(bom, directory)
    assertThat(bom.metadata.component.hashes.single().value).isEqualTo(SbomCore.hash(launcher))
    assertThat(bom.metadata.component.properties.single().value).contains("a".repeat(64))
  }

  @Test
  fun rejectsLocationsOutsideApplication() {
    assertThatThrownBy { QuarkusInputs.bind(producer("../outside.jar"), directory) }
      .hasMessageContaining("Invalid producer location")
  }
}

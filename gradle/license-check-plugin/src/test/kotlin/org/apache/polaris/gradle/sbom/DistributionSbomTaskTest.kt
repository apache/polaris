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
import java.util.Properties
import org.assertj.core.api.Assertions.assertThat
import org.gradle.testkit.runner.GradleRunner
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class DistributionSbomTaskTest {
  @TempDir lateinit var dir: File

  @Test
  fun finalizesOnceAndTracksSubjectSelection() {
    val metadata = Properties()
    javaClass.classLoader.getResourceAsStream("plugin-under-test-metadata.properties").use {
      metadata.load(requireNotNull(it))
    }
    val classpath =
      metadata.getProperty("implementation-classpath").split(File.pathSeparator).joinToString(",") {
        "'" + it.replace("\\", "\\\\").replace("'", "\\'") + "'"
      }
    File(dir, "settings.gradle").writeText("rootProject.name = 'independent-assembly'\n")
    File(dir, "build.gradle")
      .writeText(
        """
      buildscript { dependencies { classpath files($classpath) } }
      tasks.register('finalizeDocument', org.apache.polaris.gradle.sbom.FinalizeDistributionSbom) {
        producer.set(layout.projectDirectory.file('producer.json'))
        subjectFile.set(layout.projectDirectory.file('artifact.jar'))
        selectedFiles.from(layout.projectDirectory.file('dependency.jar'))
        metadataByPath.put(layout.projectDirectory.file('dependency.jar').asFile.absolutePath, providers.fileContents(layout.projectDirectory.file('selected.json')).asText.map { new groovy.json.JsonSlurper().parseText(it) })
        subject.set(new org.apache.polaris.gradle.sbom.SbomSubject(
          providers.gradleProperty('subjectGroup').getOrElse('alternate'),
          providers.gradleProperty('subjectName').getOrElse('alternate-bundle'),
          providers.gradleProperty('subjectVersion').getOrElse('2'),
          org.apache.polaris.gradle.sbom.PackageKind.SHADED_JAR,
          providers.gradleProperty('subjectClassifier').getOrElse('')))
        document.set(layout.buildDirectory.file('document.json'))
        vexUrl.set(providers.gradleProperty('vexUrl').orElse('https://assessments.invalid/polaris.vex.json'))
      }
    """
          .trimIndent() + "\n",
      )
    fun library(name: String): Map<String, Any> {
      val purl =
        aPackageURL()
          .withType("maven")
          .withNamespace("example")
          .withName(name)
          .withVersion("1")
          .withQualifier("type", "jar")
          .build()
          .canonicalize()
      return mapOf(
        "type" to "library",
        "group" to "example",
        "name" to name,
        "version" to "1",
        "purl" to purl,
        "bom-ref" to purl,
      )
    }
    val selected = File(dir, "selected.json")
    fun select(name: String) =
      selected.writeText(
        SbomCore.toJson(
          mapOf(
            "group" to "example",
            "name" to name,
            "version" to "1",
            "type" to "jar",
            "classifier" to "",
          ),
        ),
      )
    select("first")
    File(dir, "producer.json")
      .writeText(
        SbomCore.toJson(
          mapOf(
            "bomFormat" to "CycloneDX",
            "specVersion" to "1.6",
            "version" to 1,
            "metadata" to
              mapOf(
                "timestamp" to "2026-10-23T12:00:00Z",
                "component" to
                  mapOf("type" to "application", "name" to "producer", "bom-ref" to "root"),
              ),
            "components" to listOf(library("first"), library("second")),
          ),
        ),
      )
    val artifact = File(dir, "artifact.jar").apply { writeText("version one") }
    val dependency = File(dir, "dependency.jar").apply { writeText("dependency one") }
    fun run(vararg subjectProperties: String) =
      GradleRunner.create()
        .withProjectDir(dir)
        .withArguments(
          "finalizeDocument",
          "--offline",
          "--configuration-cache",
          "--no-scan",
          *subjectProperties,
        )
        .build()
    val output = File(dir, "build/document.json")
    assertThat(run().output).contains("Configuration cache entry stored")
    assertThat(run().output)
      .contains("Configuration cache entry reused", ":finalizeDocument UP-TO-DATE")
    val first = output.readText()
    artifact.writeText("version two")
    assertThat(run().output).contains("Configuration cache entry reused")
    assertThat(output.readText()).isNotEqualTo(first)
    val changedSubject = output.readText()
    dependency.writeText("dependency two")
    assertThat(run().output).contains("Configuration cache entry reused")
    assertThat(output.readText()).isNotEqualTo(changedSubject).contains(SbomCore.hash(dependency))
    select("second")
    assertThat(run().output).contains("Configuration cache entry reused")
    val document = SbomCore.readBom(output)
    assertThat(SbomCore.components(document).map { it.name })
      .containsExactly("alternate-bundle", "second")
    assertThat(document.metadata.timestamp.toInstant().toString()).isEqualTo("2026-10-23T12:00:00Z")
    var previous = output.readText()
    val overrides = mutableListOf<String>()
    for ((property, value) in
      listOf(
        "subjectGroup" to "another.group",
        "subjectName" to "another-bundle",
        "subjectVersion" to "3",
        "subjectClassifier" to "tests",
      )) {
      overrides += "-P$property=$value"
      val result = run(*overrides.toTypedArray())
      assertThat(result.output).doesNotContain(":finalizeDocument UP-TO-DATE")
      assertThat(output.readText()).isNotEqualTo(previous)
      previous = output.readText()
    }
    val root = SbomCore.readBom(output).metadata.component
    assertThat(root.group).isEqualTo("another.group")
    assertThat(root.name).isEqualTo("another-bundle")
    assertThat(root.version).isEqualTo("3")
    assertThat(root.purl)
      .isEqualTo("pkg:maven/another.group/another-bundle@3?classifier=tests&type=jar")
    assertThat(run(*overrides.toTypedArray()).output)
      .contains("Configuration cache entry reused", ":finalizeDocument UP-TO-DATE")
    overrides += "-PvexUrl=https://assessments.invalid/updated.vex.json"
    assertThat(run(*overrides.toTypedArray()).output).doesNotContain(":finalizeDocument UP-TO-DATE")
    assertThat(SbomCore.readBom(output).externalReferences.single().url)
      .isEqualTo("https://assessments.invalid/updated.vex.json")
    assertThat(run(*overrides.toTypedArray()).output)
      .contains("Configuration cache entry reused", ":finalizeDocument UP-TO-DATE")
  }
}

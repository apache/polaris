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

// Copyright (C) 2026 Dremio

package licenses

import groovy.json.JsonSlurper
import java.nio.file.Path
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.gradle.testfixtures.ProjectBuilder
import org.gradle.testkit.runner.GradleRunner
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class GenerateShadowLicenseInventoryTest {
  @Test
  fun conventionFollowsActualShadowFilterAndReusesConfigurationCache(@TempDir dir: Path) {
    dir
      .resolve("settings.gradle")
      .toFile()
      .writeText("rootProject.name = 'shadow-license-fixture'\n")
    dir
      .resolve("build.gradle")
      .toFile()
      .writeText(
        """
        import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
        import groovy.json.JsonOutput

        abstract class InspectShadowInputs extends DefaultTask {
          @InputFiles @PathSensitive(PathSensitivity.RELATIVE)
          abstract ConfigurableFileCollection getSelected()
          @Input abstract MapProperty<String, Map<String, String>> getMetadata()
          @Input abstract Property<Boolean> getMinimized()
          @OutputFile abstract RegularFileProperty getReport()

          @TaskAction void inspect() {
            def byPath = metadata.get()
            def artifacts = selected.files.collect { file ->
              assert byPath.containsKey(file.absolutePath)
              byPath[file.absolutePath]
            }.sort { it.name }
            def output = report.get().asFile
            output.parentFile.mkdirs()
            output.text = JsonOutput.toJson([minimized: minimized.get(), artifacts: artifacts])
          }
        }

        plugins {
          id 'java'
          id 'polaris-spark-license-checks'
        }
        apply plugin: 'com.gradleup.shadow'
        tasks.register('createPolarisSparkJar', ShadowJar) {
          archiveClassifier = 'bundle'
          from sourceSets.main.output
          configurations = provider { [project.configurations.runtimeClasspath] }
        }
        repositories { maven { url = uri('repository') } }
        dependencies {
          implementation 'example:included:1'
          implementation 'example:excluded:1'
          implementation 'example:classified:1:tests'
        }
        tasks.named('createPolarisSparkJar') {
          dependencies {
            include(dependency('example:included:.*'))
            include(dependency('example:classified:.*'))
          }
        }
        def shadowInputs = extensions.extraProperties.get('sbomShadow')
        tasks.register('inspectShadowInputs', InspectShadowInputs) {
          selected.from(shadowInputs.selected)
          metadata.set(shadowInputs.metadata)
          minimized.set(shadowInputs.minimized)
          report.set(layout.buildDirectory.file('shadow-inputs.json'))
        }
        """
          .trimIndent() + "\n",
      )
    for (name in listOf("included", "excluded", "classified")) {
      val repository = dir.resolve("repository/example/$name/1").toFile().apply { mkdirs() }
      repository
        .resolve("$name-1.pom")
        .writeText(
          """
          <project><modelVersion>4.0.0</modelVersion><groupId>example</groupId>
          <artifactId>$name</artifactId><version>1</version></project>
          """
            .trimIndent() + "\n",
        )
      val classifier = if (name == "classified") "-tests" else ""
      ZipOutputStream(repository.resolve("$name-1$classifier.jar").outputStream()).use { zip ->
        zip.putNextEntry(ZipEntry("$name.txt"))
        zip.write(name.toByteArray())
        zip.closeEntry()
      }
    }
    val runner =
      GradleRunner.create()
        .withProjectDir(dir.toFile())
        .withPluginClasspath()
        .withArguments(
          "generateLicenseInventory",
          "inspectShadowInputs",
          "--configuration-cache",
          "--offline",
          "--no-scan",
        )
    val first = runner.build()
    assertThat(first.output).contains("Configuration cache entry stored.")
    assertThat(first.task(":createPolarisSparkJar")).isNull()
    assertThat(runner.build().output).contains("Configuration cache entry reused.")
    assertThat(dir.resolve("build/reports/licensee/shadow-inputs.json").toFile().readText())
      .contains("pkg:maven/example/included@1", "pkg:maven/example/classified@1")
      .doesNotContain("excluded")
    val inspected =
      JsonSlurper().parse(dir.resolve("build/shadow-inputs.json").toFile()) as Map<*, *>
    assertThat(inspected["minimized"]).isEqualTo(false)
    assertThat(inspected["artifacts"])
      .isEqualTo(
        listOf(
          mapOf(
            "group" to "example",
            "name" to "classified",
            "version" to "1",
            "classifier" to "tests",
            "type" to "jar",
          ),
          mapOf(
            "group" to "example",
            "name" to "included",
            "version" to "1",
            "classifier" to "",
            "type" to "jar",
          ),
        ),
      )
  }

  @Test
  fun recordsSelectedInputsWithoutAddingExcludedRuntimeDependencies(@TempDir dir: Path) {
    val task = fixture(dir)
    task.generate()
    assertThat(task.inventory.get().asFile.readText())
      .contains("pkg:maven/example/selected@1")
      .doesNotContain("excluded")
  }

  @Test
  fun rejectsUnmappedSelectedDependency(@TempDir dir: Path) {
    val task = fixture(dir)
    task.coordinatesByPath.set(emptyMap())
    assertThatThrownBy(task::generate)
      .isInstanceOf(IllegalArgumentException::class.java)
      .hasMessageContaining("no resolved Maven coordinate")
  }

  @Test
  fun rejectsEmptyProducerInputSet(@TempDir dir: Path) {
    val task = fixture(dir)
    task.selectedDependencies.setFrom(emptyList<Any>())
    assertThatThrownBy(task::generate)
      .isInstanceOf(IllegalArgumentException::class.java)
      .hasMessageContaining("nonempty Shadow dependency inputs")
  }

  @Test
  fun rejectsUnreviewedMinimization(@TempDir dir: Path) {
    val task = fixture(dir)
    task.minimized.set(true)
    assertThatThrownBy(task::generate)
      .isInstanceOf(IllegalArgumentException::class.java)
      .hasMessageContaining("Minimized Shadow packaging requires")
  }

  private fun fixture(dir: Path): GenerateShadowLicenseInventory {
    val project = ProjectBuilder.builder().withProjectDir(dir.toFile()).build()
    val task = project.tasks.register("inventory", GenerateShadowLicenseInventory::class.java).get()
    val selected = dir.resolve("selected.jar").toFile().apply { writeText("fixture") }
    val excluded = dir.resolve("excluded.jar").toFile().apply { writeText("fixture") }
    task.selectedDependencies.from(selected)
    task.coordinatesByPath.set(
      mapOf(
        selected.absolutePath to "example:selected:1",
        excluded.absolutePath to "example:excluded:2",
      ),
    )
    task.minimized.set(false)
    task.inventory.set(dir.resolve("inventory.json").toFile())
    return task
  }
}

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

import groovy.json.JsonOutput
import org.gradle.api.DefaultTask
import org.gradle.api.Project
import org.gradle.api.attributes.Category
import org.gradle.api.attributes.VerificationType
import org.gradle.api.file.FileCollection
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Provider
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.api.tasks.scala.ScalaCompile
import org.gradle.api.tasks.scala.ScalaDoc
import org.gradle.jvm.toolchain.JavaInstallationMetadata
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.register
import org.gradle.kotlin.dsl.withType
import org.gradle.work.DisableCachingByDefault

@DisableCachingByDefault(because = "Small diagnostic report of configured toolchains")
abstract class ReleaseToolchains : DefaultTask() {
  @get:Input abstract val toolchains: ListProperty<Map<String, String>>
  @get:OutputFile abstract val outputFile: RegularFileProperty

  @TaskAction
  fun generate() {
    val file = outputFile.get().asFile
    file.parentFile.mkdirs()
    file.writeText(JsonOutput.toJson(toolchains.get().sortedBy { it.getValue("task") }) + "\n")
  }
}

/** Publishes metadata from this project's tasks without accessing another project's model. */
fun configureReleaseToolchains(project: Project): Unit = project.run {
  val projectTasks = tasks
  val report =
    tasks.register<ReleaseToolchains>("releaseToolchains") {
      toolchains.convention(emptyList())
      outputFile.set(layout.buildDirectory.file("reports/release-toolchains.json"))

      fun record(taskPath: String, role: String, metadata: Provider<JavaInstallationMetadata>) {
        toolchains.add(
          metadata.map {
            mapOf(
              "task" to taskPath,
              "role" to role,
              "vendor" to it.vendor,
              "runtimeVersion" to it.javaRuntimeVersion,
              "languageVersion" to it.languageVersion.toString(),
            )
          },
        )
      }
      projectTasks.withType<JavaCompile>().forEach {
        record(it.path, "javaCompiler", it.javaCompiler.map { compiler -> compiler.metadata })
      }
      projectTasks.withType<Javadoc>().forEach {
        record(it.path, "javadoc", it.javadocTool.map { tool -> tool.metadata })
      }
      projectTasks.withType<ScalaCompile>().forEach {
        record(it.path, "scalaCompiler", it.javaLauncher.map { launcher -> launcher.metadata })
      }
      projectTasks.withType<ScalaDoc>().forEach {
        record(it.path, "scaladoc", it.javaLauncher.map { launcher -> launcher.metadata })
      }
    }
  configurations.consumable("releaseToolchainsElements") {
    attributes {
      attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.VERIFICATION))
      attribute(
        VerificationType.VERIFICATION_TYPE_ATTRIBUTE,
        objects.named("polaris-release-toolchains"),
      )
    }
    outgoing.artifact(report.flatMap { it.outputFile })
  }
}

/** Aggregates report artifacts; only immutable project paths are read across project boundaries. */
fun releaseToolchainReports(project: Project): FileCollection = project.run {
  val reportDependencies =
    configurations.dependencyScope("releaseToolchainDependencies") {
      subprojects.forEach {
        dependencies.add(project.dependencies.project(mapOf("path" to it.path)))
      }
    }
  val reports =
    configurations.resolvable("releaseToolchainReports") {
      extendsFrom(reportDependencies.get())
      isTransitive = false
      attributes {
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.VERIFICATION))
        attribute(
          VerificationType.VERIFICATION_TYPE_ATTRIBUTE,
          objects.named("polaris-release-toolchains"),
        )
      }
    }
  // Projects without Java tasks do not publish this verification variant.
  reports.get().incoming.artifactView { isLenient = true }.files
}

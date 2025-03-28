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

import org.gradle.api.DefaultTask
import org.gradle.api.Project
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.plugins.JavaLibraryPlugin
import org.gradle.api.provider.ListProperty
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.Sync
import org.gradle.api.tasks.TaskAction
import org.gradle.kotlin.dsl.provideDelegate

@CacheableTask
abstract class GeneratePomProperties : DefaultTask() {
  @Suppress("unused") @get:Input abstract val pomInputs: ListProperty<String>

  @get:OutputDirectory abstract val destinationDir: DirectoryProperty

  init {
    pomInputs.convention(listOf(project.group.toString(), project.name, project.version.toString()))
    destinationDir.convention(project.layout.buildDirectory.dir("generated/pom-properties"))
  }

  @TaskAction
  fun generate() {
    val buildDir = destinationDir.get().asFile
    buildDir.deleteRecursively()
    val targetDir = buildDir.resolve("META-INF/maven/${project.group}/${project.name}")
    targetDir.mkdirs()
    targetDir
      .resolve("pom.properties")
      .writeText(
        """
      # Generated by the Apache Polaris build.
      groupId=${project.group}
      artifactId=${project.name}
      version=${project.version}
    """
          .trimIndent()
      )
  }
}

/**
 * Adds convenient, but not strictly necessary information to each generated "main" jar.
 *
 * This includes `pom.properties` and `pom.xml` files where Maven places those, in
 * `META-INF/maven/group-id/artifact-id/`. Also adds the `NOTICE` and `LICENSE` files in `META-INF`,
 * which makes it easier for license scanners.
 */
fun addAdditionalJarContent(project: Project): Unit =
  project.run {
    project.plugins.withType(JavaLibraryPlugin::class.java) {
      val generatePomProperties =
        tasks.register("generatePomProperties", GeneratePomProperties::class.java) {}

      val additionalJarContent =
        tasks.register("additionalJarContent", Sync::class.java) {
          // Have to manually declare the inputs of this task here on top of the from/include below
          inputs.files(rootProject.layout.files("LICENSE", "NOTICE"))
          inputs.property("GAV", "${project.group}:${project.name}:${project.version}")
          dependsOn("generatePomFileForMavenPublication")
          from(rootProject.rootDir) {
            include("LICENSE", "NOTICE")
            eachFile {
              this.path =
                "META-INF/licenses/${project.group}/${project.name}-${project.version}/$sourceName"
            }
          }
          from(tasks.named("generatePomFileForMavenPublication")) {
            include("pom-default.xml")
            eachFile { this.path = "META-INF/maven/${project.group}/${project.name}/pom.xml" }
          }
          into(layout.buildDirectory.dir("license-for-jar"))
        }

      tasks.named("processResources") { dependsOn("additionalJarContent") }

      val sourceSets: SourceSetContainer by project
      sourceSets.named("main") {
        resources.srcDir(additionalJarContent)
        resources.srcDir(generatePomProperties)
      }
    }
  }

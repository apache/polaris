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

plugins {
  `java-library`
  id("polaris-reproducible")
}

description = "Polaris site - reference docs"

val genProjectPaths = listOf(
  ":polaris-async-api",
  ":polaris-core",
  ":polaris-extensions-auth-opa",
  ":polaris-nodes-api",
  ":polaris-persistence-nosql-api",
  ":polaris-persistence-nosql-impl",
  ":polaris-persistence-nosql-cdi-quarkus",
  ":polaris-persistence-nosql-cdi-quarkus-distcache",
  ":polaris-persistence-nosql-inmemory",
  ":polaris-persistence-nosql-maintenance-api",
  ":polaris-persistence-nosql-metastore-maintenance",
  ":polaris-persistence-nosql-mongodb",
  ":polaris-persistence-nosql-metastore-types",
  ":polaris-runtime-common",
  ":polaris-runtime-service",
)

val genProjects by configurations.creating
val genSources by configurations.creating
val doclet by configurations.creating

dependencies {
  doclet(project(":polaris-config-docs-annotations"))
  doclet(project(":polaris-config-docs-generator"))
  doclet(project(":polaris-core"))
  doclet(libs.smallrye.config.core)

  genProjects(project(":polaris-config-docs-annotations"))

  genProjectPaths.forEach { p ->
    genProjects(project(p))
    genSources(project(p, "mainSourceElements"))
  }
}

val generatedMarkdownDocsDir = layout.buildDirectory.dir("generatedMarkdownDocs")

val generatedMarkdownDocs = tasks.register<JavaExec>("generatedMarkdownDocs") {

  mainClass = "org.apache.polaris.docs.generator.ReferenceConfigDocsGenerator"

  val genMdDocsDir = generatedMarkdownDocsDir.map { it.asFile.relativeTo(projectDir) }
  outputs.cacheIf { true }
  outputs.dir(genMdDocsDir)
  inputs.files(doclet).withNormalizer(ClasspathNormalizer::class.java)
  inputs.files(genProjects).withNormalizer(ClasspathNormalizer::class.java)
  inputs.files(genSources).withNormalizer(ClasspathNormalizer::class.java)

  doFirst(CleanDirectoryAction(generatedMarkdownDocsDir))

  argumentProviders.add(
    ConfigDocsArgumentProvider(genProjects, genSources, generatedMarkdownDocsDir, layout.projectDirectory)
  )

  classpath(doclet)
}

val generateDocs by tasks.registering(Sync::class) {
  dependsOn(generatedMarkdownDocs)

  val targetDir = layout.buildDirectory.dir("markdown-docs")

  outputs.dir(targetDir)

  into(targetDir)

  from(generatedMarkdownDocsDir)
  exclude("org/**")

  duplicatesStrategy = DuplicatesStrategy.FAIL
}

val copyConfigSectionsToSite by tasks.registering(CopyConfigSectionsToSite::class) {
  dependsOn(generateDocs)

  description = "Copies the generated configuration section files to the site content directory as a headless bundle"
  group = "documentation"

  sourceDir.set(layout.buildDirectory.dir("markdown-docs"))
  targetDir.set(
    rootProject.layout.projectDirectory.dir("site/content/in-dev/unreleased/configuration/config-sections")
  )
}

tasks.named("assemble") {
  dependsOn(copyConfigSectionsToSite)
}

class CleanDirectoryAction(private val directory: Provider<Directory>) : Action<Task> {
  override fun execute(task: Task) {
    directory.get().asFile.deleteRecursively()
  }
}

class ConfigDocsArgumentProvider(
  @get:Classpath val classes: FileCollection,
  @get:InputFiles @get:PathSensitive(PathSensitivity.RELATIVE) val sources: FileCollection,
  @get:Internal val destinationDir: Provider<Directory>,
  @get:Internal val projectDirectory: Directory,
) : CommandLineArgumentProvider {
  override fun asArguments(): Iterable<String> =
    listOf(
      "--classpath",
      classes.files.joinToString(":") { it.relativeTo(projectDirectory.asFile).toString() },
      "--sourcepath",
      sources.files
        .filter { it.name != "resources" }
        .joinToString(":") { it.relativeTo(projectDirectory.asFile).toString() },
      "--destination",
      destinationDir.get().asFile.relativeTo(projectDirectory.asFile).toString(),
    )
}

abstract class CopyConfigSectionsToSite : DefaultTask() {
  @get:InputDirectory
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val sourceDir: DirectoryProperty

  @get:OutputDirectory abstract val targetDir: DirectoryProperty

  @TaskAction
  fun copyConfigSections() {
    val target = targetDir.get().asFile
    target.mkdirs()
    target
      .listFiles()
      ?.filter { it.name.endsWith(".md") && it.name != "_index.md" }
      ?.forEach { it.delete() }

    sourceDir
      .asFileTree
      .matching {
        include("smallrye-*.md")
        include("flags-*.md")
      }
      .files
      .forEach { file ->
        target
          .resolve(file.name)
          .writeText(frontMatter(file.nameWithoutExtension) + file.readText())
      }
  }

  private fun frontMatter(title: String): String =
    """---
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
title: $title
build:
  list: never
  render: never
---

"""
}

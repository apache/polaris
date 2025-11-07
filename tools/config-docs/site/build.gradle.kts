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
  ":polaris-runtime-service",
)

val genProjects by configurations.creating
val genSources by configurations.creating
val doclet by configurations.creating

dependencies {
  doclet(project(":polaris-config-docs-annotations"))
  doclet(project(":polaris-config-docs-generator"))
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

  outputs.cacheIf { true }
  outputs.dir(generatedMarkdownDocsDir)
  inputs.files(doclet)
  inputs.files(genProjects)
  inputs.files(genSources)

  doFirst {
    delete(generatedMarkdownDocsDir)
  }

  argumentProviders.add(CommandLineArgumentProvider {

    // So, in theory, all 'org.gradle.category' attributes should use the type
    // org.gradle.api.attributes.Category,
    // as Category.CATEGORY_ATTRIBUTE is defined. BUT! Some attributes have an attribute type ==
    // String.class!
    val categoryAttributeAsString = Attribute.of("org.gradle.category", String::class.java)

    val classes = genProjects.incoming.artifacts
      .filter { a ->
        // dependencies:
        //  org.gradle.category=library
        val category =
          a.variant.attributes.getAttribute(Category.CATEGORY_ATTRIBUTE)
            ?: a.variant.attributes.getAttribute(categoryAttributeAsString)
        category != null && category.toString() == Category.LIBRARY
      }
      .map { a -> a.file }

    val sources = genSources.incoming.artifacts
      .filter { a ->
        // sources:
        //  org.gradle.category=verification
        //  org.gradle.verificationtype=main-sources

        val category = a.variant.attributes.getAttribute(Category.CATEGORY_ATTRIBUTE)
        val verificationType =
          a.variant.attributes.getAttribute(VerificationType.VERIFICATION_TYPE_ATTRIBUTE)
        category != null &&
          category.name == Category.VERIFICATION &&
          verificationType != null &&
          verificationType.name == VerificationType.MAIN_SOURCES &&
          a.file.name != "resources"
      }
      .map { a -> a.file }

    listOf(
      "--classpath", classes.joinToString(":"),
      "--sourcepath", sources.joinToString(":"),
      "--destination", generatedMarkdownDocsDir.get().toString()
    ) + (if (logger.isInfoEnabled) listOf("--verbose") else listOf())
  })

  classpath(doclet)
}

val generateDocs by tasks.registering(Sync::class) {
  dependsOn(generatedMarkdownDocs)

  val targetDir = layout.buildDirectory.dir("markdown-docs")

  outputs.dir(targetDir)

  into(targetDir)

  from(generatedMarkdownDocsDir)

  duplicatesStrategy = DuplicatesStrategy.FAIL

  doLast { delete(targetDir.get().dir("org")) }
}

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
  id("polaris-server")
  id("polaris-checkstyle")
}

description = "Generates Polaris reference docs"

val genTesting by configurations.creating

dependencies {
  implementation(project(":polaris-config-docs-annotations"))

  implementation(libs.commons.text)

  implementation(libs.smallrye.config.core)
  implementation(libs.picocli)
  annotationProcessor(libs.picocli.codegen)

  genTesting(project(":polaris-config-docs-annotations"))
  genTesting(libs.smallrye.config.core)
}

tasks.named<Test>("test") {
  // The test needs the classpath for the necessary dependencies (annotations + smallrye-config).
  // Resolving the dependencies must happen during task execution (not configuration).
  jvmArgumentProviders.add(
    CommandLineArgumentProvider {
      // So, in theory, all 'org.gradle.category' attributes should use the type
      // org.gradle.api.attributes.Category,
      // as Category.CATEGORY_ATTRIBUTE is defined. BUT! Some attributes have an attribute type ==
      // String.class!

      val categoryAttributeAsString = Attribute.of("org.gradle.category", String::class.java)

      val libraries =
        genTesting.incoming.artifacts
          .filter { a ->
            // dependencies:
            //  org.gradle.category=library
            val category =
              a.variant.attributes.getAttribute(Category.CATEGORY_ATTRIBUTE)
                ?: a.variant.attributes.getAttribute(categoryAttributeAsString)
            category != null && category.toString() == Category.LIBRARY
          }
          .map { a -> a.file }

      listOf("-Dtesting.libraries=" + libraries.joinToString(":"))
    }
  )
}

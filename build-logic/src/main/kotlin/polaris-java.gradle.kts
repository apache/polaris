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

import net.ltgt.gradle.errorprone.errorprone
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.named
import publishing.PublishingHelperPlugin

plugins {
  id("jacoco")
  id("java")
  id("com.diffplug.spotless")
  id("jacoco-report-aggregation")
  id("net.ltgt.errorprone")
}

apply<PublishingHelperPlugin>()

tasks.withType(JavaCompile::class.java).configureEach {
  options.compilerArgs.addAll(listOf("-Xlint:unchecked", "-Xlint:deprecation"))
  options.errorprone.disableAllWarnings = true
  options.errorprone.disableWarningsInGeneratedCode = true
  options.errorprone.error(
    "DefaultCharset",
    "FallThrough",
    "MissingCasesInEnumSwitch",
    "MissingOverride",
    "ModifiedButNotUsed",
    "OrphanedFormatString",
    "PatternMatchingInstanceof",
    "StringCaseLocaleUsage",
  )
}

tasks.register("compileAll").configure {
  group = "build"
  description = "Runs all compilation and jar tasks"
  dependsOn(tasks.withType<AbstractCompile>(), tasks.withType<ProcessResources>())
}

tasks.register("format").configure {
  group = "verification"
  description = "Runs all code formatting tasks"
  dependsOn("spotlessApply")
}

tasks.named<Test>("test").configure {
  useJUnitPlatform()
  jvmArgs("-Duser.language=en")
}

spotless {
  val disallowWildcardImports = { text: String ->
    val regex = "~/import .*\\.\\*;/".toRegex()
    if (regex.matches(text)) {
      throw GradleException("Wildcard imports disallowed - ${regex.findAll(text)}")
    }
    text
  }
  java {
    target("src/main/java/**/*.java", "src/testFixtures/java/**/*.java", "src/test/java/**/*.java")
    googleJavaFormat()
    licenseHeaderFile(rootProject.file("codestyle/copyright-header-java.txt"))
    endWithNewline()
    custom("disallowWildcardImports", disallowWildcardImports)
    toggleOffOn()
  }
  kotlinGradle {
    ktfmt().googleStyle()
    licenseHeaderFile(rootProject.file("codestyle/copyright-header-java.txt"), "$")
    target("*.gradle.kts")
  }
  format("xml") {
    target("src/**/*.xml", "src/**/*.xsd")
    targetExclude("codestyle/copyright-header.xml")
    eclipseWtp(com.diffplug.spotless.extra.wtp.EclipseWtpFormatterStep.XML)
      .configFile(rootProject.file("codestyle/org.eclipse.wst.xml.core.prefs"))
    // getting the license-header delimiter right is a bit tricky.
    // licenseHeaderFile(rootProject.file("codestyle/copyright-header.xml"), '<^[!?].*$')
  }
}

dependencies { errorprone(versionCatalogs.named("libs").findLibrary("errorprone").get()) }

java {
  withJavadocJar()
  withSourcesJar()
}

tasks.withType<Javadoc>().configureEach {
  val opt = options as CoreJavadocOptions
  // don't spam log w/ "warning: no @param/@return"
  opt.addStringOption("Xdoclint:-reference", "-quiet")
}

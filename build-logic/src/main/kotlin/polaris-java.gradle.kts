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

import com.diffplug.spotless.FormatterFunc
import java.io.Serializable
import net.ltgt.gradle.errorprone.errorprone
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.named
import publishing.PublishingHelperPlugin

plugins {
  jacoco
  `java-library`
  `java-test-fixtures`
  `jvm-test-suite`
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

tasks.named<Test>("test").configure { jvmArgs("-Duser.language=en") }

testing {
  suites {
    withType<JvmTestSuite> {
      val libs = versionCatalogs.named("libs")

      useJUnitJupiter(
        libs
          .findLibrary("junit-bom")
          .orElseThrow { GradleException("junit-bom not declared in libs.versions.toml") }
          .map { it.version!! }
      )

      dependencies {
        implementation(project())
        implementation(testFixtures(project()))
        runtimeOnly(
          libs.findLibrary("logback-classic").orElseThrow {
            GradleException("logback-classic not declared in libs.versions.toml")
          }
        )
        implementation(
          libs.findLibrary("assertj-core").orElseThrow {
            GradleException("assertj-core not declared in libs.versions.toml")
          }
        )
        implementation(
          libs.findLibrary("mockito-core").orElseThrow {
            GradleException("mockito-core not declared in libs.versions.toml")
          }
        )
      }

      targets.all {
        if (testTask.name != "test") {
          testTask.configure { shouldRunAfter("test") }
        }
      }
    }
  }
}

dependencies {
  val libs = versionCatalogs.named("libs")
  testFixturesImplementation(
    platform(
      libs.findLibrary("junit-bom").orElseThrow {
        GradleException("junit-bom not declared in libs.versions.toml")
      }
    )
  )
  testFixturesImplementation("org.junit.jupiter:junit-jupiter")
  testFixturesImplementation(
    libs.findLibrary("assertj-core").orElseThrow {
      GradleException("assertj-core not declared in libs.versions.toml")
    }
  )
  testFixturesImplementation(
    libs.findLibrary("mockito-core").orElseThrow {
      GradleException("mockito-core not declared in libs.versions.toml")
    }
  )
}

tasks.withType(Jar::class).configureEach {
  manifest {
    attributes(
      // Do not add any (more or less) dynamic information to jars, because that makes Gradle's
      // caching way less efficient. Note that version and Git information are already added to jar
      // manifests for release(-like) builds.
      "Implementation-Title" to "Apache Polaris(TM) (incubating)",
      "Implementation-Vendor" to "Apache Software Foundation",
      "Implementation-URL" to "https://polaris.apache.org/",
    )
  }
}

spotless {
  java {
    target("src/main/java/**/*.java", "src/testFixtures/java/**/*.java", "src/test/java/**/*.java")
    googleJavaFormat()
    licenseHeaderFile(rootProject.file("codestyle/copyright-header-java.txt"))
    endWithNewline()
    custom(
      "disallowWildcardImports",
      object : Serializable, FormatterFunc {
        override fun apply(text: String): String {
          val regex = "~/import .*\\.\\*;/".toRegex()
          if (regex.matches(text)) {
            throw GradleException("Wildcard imports disallowed - ${regex.findAll(text)}")
          }
          return text
        }
      },
    )
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

tasks.register("printRuntimeClasspath").configure {
  group = "help"
  description = "Print the classpath as a path string to be used when running tools like 'jol'"
  inputs.files(configurations.named("runtimeClasspath"))
  doLast {
    val cp = configurations.getByName("runtimeClasspath")
    val def = configurations.getByName("runtimeElements")
    logger.lifecycle("${def.outgoing.artifacts.files.asPath}:${cp.asPath}")
  }
}

configurations.all {
  rootProject
    .file("gradle/banned-dependencies.txt")
    .readText(Charsets.UTF_8)
    .trim()
    .lines()
    .map { it.trim() }
    .filterNot { it.isBlank() || it.startsWith("#") }
    .forEach { line ->
      val idx = line.indexOf(':')
      if (idx == -1) {
        exclude(group = line)
      } else {
        val group = line.substring(0, idx)
        val module = line.substring(idx + 1)
        exclude(group = group, module = module)
      }
    }
}

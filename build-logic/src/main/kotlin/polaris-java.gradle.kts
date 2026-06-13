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

import java.util.Properties
import kotlin.jvm.java
import net.ltgt.gradle.errorprone.CheckSeverity
import net.ltgt.gradle.errorprone.errorprone
import org.gradle.api.file.FileCollection
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.services.BuildService
import org.gradle.api.services.BuildServiceParameters
import org.gradle.api.tasks.Classpath
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.named
import org.gradle.process.CommandLineArgumentProvider
import org.kordamp.gradle.plugin.jandex.JandexExtension
import org.kordamp.gradle.plugin.jandex.JandexPlugin
import publishing.PublishingHelperPlugin

plugins {
  jacoco
  `java-library`
  `java-test-fixtures`
  `jvm-test-suite`
  checkstyle
  id("polaris-base")
  id("polaris-spotless")
  id("polaris-reproducible")
  id("jacoco-report-aggregation")
  id("net.ltgt.errorprone")
}

apply<PublishingHelperPlugin>()

private val libs: VersionCatalog by lazy { versionCatalogs.named("libs") }

private fun requiredLib(name: String): Provider<MinimalExternalModuleDependency> =
  libs.findLibrary(name).orElseThrow {
    GradleException("$name version not found in libs.versions.toml")
  }

plugins.withType<JandexPlugin>().configureEach {
  extensions.getByType(JandexExtension::class).run {
    version = requiredLib("smallrye-jandex").get().version
    // https://smallrye.io/jandex/jandex/3.4.0/index.html#persistent_index_format_versions
    indexVersion = 12
  }
}

checkstyle {
  val checkstyleVersion =
    versionCatalogs
      .named("libs")
      .findVersion("checkstyle")
      .orElseThrow { GradleException("checkstyle version not found in libs.versions.toml") }
      .requiredVersion
  toolVersion = checkstyleVersion
  configFile = rootProject.file("codestyle/checkstyle.xml")
  isIgnoreFailures = false
  maxErrors = 0
  maxWarnings = 0
}

tasks.withType<Checkstyle>().configureEach {
  if (noSourceCheckProjects.contains(project.path)) {
    enabled = false
  } else {
    // Ensure Checkstyle runs after jandex to avoid task dependency issues
    tasks.findByName("jandex")?.let { mustRunAfter(it) }
  }
}

tasks.withType(JavaCompile::class.java).configureEach {
  options.compilerArgs.addAll(
    listOf("-Xlint:unchecked", "-Xlint:deprecation", "-XDaddTypeAnnotationsToSymbol=true")
  )
  options.errorprone.disableAllWarnings = true
  options.errorprone.disableWarningsInGeneratedCode = true
  options.errorprone.excludedPaths =
    ".*/${project.layout.buildDirectory.get().asFile.relativeTo(projectDir)}/generated(-openapi)?/.*"
  val errorproneRules = rootProject.projectDir.resolve("codestyle/errorprone-rules.properties")
  inputs.file(errorproneRules).withPathSensitivity(PathSensitivity.RELATIVE)
  options.errorprone.checks.putAll(provider { memoizedErrorproneRules(errorproneRules) })
}

private fun memoizedErrorproneRules(rulesFile: File): Map<String, CheckSeverity> =
  rulesFile.reader().use {
    val rules = Properties()
    rules.load(it)
    rules
      .mapKeys { e -> (e.key as String).trim() }
      .mapValues { e -> (e.value as String).trim() }
      .filter { e -> e.key.isNotEmpty() && e.value.isNotEmpty() }
      .mapValues { e -> CheckSeverity.valueOf(e.value) }
      .toMap()
  }

tasks.register("compileAll") {
  group = "build"
  description = "Runs all compilation and jar tasks"
  dependsOn(tasks.withType<AbstractCompile>(), tasks.withType<ProcessResources>())
}

tasks.register("format") {
  group = "verification"
  description = "Runs all code formatting tasks"
  dependsOn("spotlessApply")
}

tasks.named<Test>("test") { jvmArgs("-Duser.language=en") }

testing {
  suites {
    @Suppress("UnstableApiUsage")
    withType<JvmTestSuite> {
      useJUnitJupiter(requiredLib("junit-bom").map { it.version!! })

      dependencies {
        implementation(project())
        implementation(testFixtures(project()))
        if (!plugins.hasPlugin("io.quarkus")) {
          implementation(requiredLib("logback-classic"))
        }
        implementation(requiredLib("assertj-core"))
        implementation(requiredLib("mockito-core"))
      }

      // Special handling for test-suites with names containing `manualtest`, which are intended to
      // be run on demand rather than implicitly via `check`.
      if (!name.lowercase().contains("manualtest")) {
        targets.all {
          if (testTask.name != "test") {
            testTask.configure { shouldRunAfter("test") }
            tasks.named("check").configure { dependsOn(testTask) }
          }
        }
      }
    }
  }
}

val mockitoAgent = configurations.create("mockitoAgent")

dependencies {
  testFixturesImplementation(platform(requiredLib("junit-bom")))
  testFixturesImplementation("org.junit.jupiter:junit-jupiter")
  testFixturesImplementation(requiredLib("assertj-core"))
  val mockitoCoreLib = requiredLib("mockito-core")

  testFixturesImplementation(mockitoCoreLib)

  mockitoAgent(mockitoCoreLib) { isTransitive = false }
}

private class MockitoJavaAgentArgumentProvider(@get:Classpath val mockitoAgent: FileCollection) :
  CommandLineArgumentProvider {
  override fun asArguments(): Iterable<String> = listOf("-javaagent:${mockitoAgent.asPath}")
}

tasks.withType<Test>().configureEach {
  systemProperty("file.encoding", "UTF-8")
  systemProperty("user.language", "en")
  systemProperty("user.country", "US")
  systemProperty("user.variant", "")
  jvmArgumentProviders.add(MockitoJavaAgentArgumentProvider(mockitoAgent))
}

tasks.withType<Jar>().configureEach {
  manifest {
    attributes(
      // Do not add any (more or less) dynamic information to jars, because that makes Gradle's
      // caching way less efficient. Note that version and Git information are already added to jar
      // manifests for release(-like) builds.
      "Implementation-Title" to "Apache Polaris(TM)",
      "Implementation-Vendor" to "Apache Software Foundation",
      "Implementation-URL" to "https://polaris.apache.org/",
    )
  }

  if (!project.file("src/main/no-license-notice-marker").exists()) {
    if (!project.file("src/main/resources/META-INF/LICENSE").exists()) {
      from(rootProject.rootDir) {
        include("gradle/jar-licenses/LICENSE").eachFile { path = "META-INF/$sourceName" }
      }
    } else if (name == "javadocJar") {
      from("src/main/resources") { include("META-INF/LICENSE") }
    }
    if (!project.file("src/main/resources/META-INF/NOTICE").exists()) {
      from(rootProject.rootDir) {
        include("gradle/jar-licenses/NOTICE").eachFile { path = "META-INF/$sourceName" }
      }
    } else if (name == "javadocJar") {
      from("src/main/resources") { include("META-INF/NOTICE") }
    }
  }
}

dependencies { errorprone(requiredLib("errorprone")) }

java {
  withJavadocJar()
  withSourcesJar()
}

tasks.withType<Javadoc>().configureEach {
  val opt = options as CoreJavadocOptions
  // don't spam log w/ "warning: no @param/@return"
  opt.addStringOption("Xdoclint:-reference", "-quiet")
  if (plugins.hasPlugin("org.kordamp.gradle.jandex")) {
    dependsOn("jandex")
  }
}

tasks.register("printRuntimeClasspath") {
  group = "help"
  description = "Print the classpath as a path string to be used when running tools like 'jol'"
  inputs
    .files(configurations.named("runtimeClasspath"))
    .withNormalizer(ClasspathNormalizer::class.java)
  doLast {
    val cp = configurations.getByName("runtimeClasspath")
    val def = configurations.getByName("runtimeElements")
    logger.lifecycle("${def.outgoing.artifacts.files.asPath}:${cp.asPath}")
  }
}

class BannedDependency(val group: String, val module: String?) {
  fun exclude(configuration: Configuration) = configuration.exclude(group = group, module = module)

  companion object {
    fun parseList(file: File): List<BannedDependency> =
      file
        .readText(Charsets.UTF_8)
        .trim()
        .lines()
        .map { it.trim() }
        .filterNot { it.isBlank() || it.startsWith("#") }
        .map { line ->
          val idx = line.indexOf(':')
          if (idx == -1) {
            BannedDependency(line, null)
          } else {
            val group = line.substring(0, idx)
            val module = line.substring(idx + 1)
            BannedDependency(group, module)
          }
        }
  }
}

class BannedDependencies(
  val globallyBanned: List<BannedDependency>,
  val quarkusProdBanned: List<BannedDependency>,
) {
  fun applyTo(configuration: Configuration) {
    if (configuration.state == Configuration.State.UNRESOLVED) {
      globallyBanned.forEach { it.exclude(configuration) }
      if (configuration.name.startsWith("quarkusProd")) {
        quarkusProdBanned.forEach { it.exclude(configuration) }
      }
    }
  }

  fun applyTo(configurations: ConfigurationContainer) {
    configurations.all { applyTo(this) }
  }
}

fun bannedDependencies(): BannedDependencies {
  val service =
    gradle.sharedServices.registerIfAbsent(
      "bannedDependencies",
      BannedDependenciesService::class.java,
    ) {
      parameters.globallyBannedFile.set(
        rootProject.layout.projectDirectory.file("gradle/banned-dependencies.txt")
      )
      parameters.quarkusProdBannedFile.set(
        rootProject.layout.projectDirectory.file("gradle/banned-quarkus-prod-dependencies.txt")
      )
    }
  return service.get().bannedDependencies
}

abstract class BannedDependenciesService : BuildService<BannedDependenciesService.Parameters> {
  interface Parameters : BuildServiceParameters {
    val globallyBannedFile: RegularFileProperty
    val quarkusProdBannedFile: RegularFileProperty
  }

  val bannedDependencies: BannedDependencies by lazy {
    BannedDependencies(
      BannedDependency.parseList(parameters.globallyBannedFile.asFile.get()),
      BannedDependency.parseList(parameters.quarkusProdBannedFile.asFile.get()),
    )
  }
}

bannedDependencies().applyTo(configurations)

gradle.sharedServices.registerIfAbsent(
  "intTestParallelismConstraint",
  TestingParallelismHelper::class.java,
) {
  val intTestParallelism =
    Integer.getInteger(
      "polaris.intTestParallelism",
      (Runtime.getRuntime().availableProcessors() / 4).coerceAtLeast(1),
    )
  maxParallelUsages = intTestParallelism
}

gradle.sharedServices.registerIfAbsent(
  "testParallelismConstraint",
  TestingParallelismHelper::class.java,
) {
  val testParallelism =
    Integer.getInteger(
      "polaris.testParallelism",
      (Runtime.getRuntime().availableProcessors() / 2).coerceAtLeast(1),
    )
  maxParallelUsages = testParallelism
}

abstract class TestingParallelismHelper : BuildService<BuildServiceParameters.None>

tasks.withType<Test>().configureEach {
  val isTestTask = name == "test"
  val constraintName =
    if (isTestTask) "testParallelismConstraint" else "intTestParallelismConstraint"
  usesService(gradle.sharedServices.registrations.named(constraintName).get().service)
  if (project.hasProperty("noIntegrationTests") && !isTestTask) {
    enabled = false
  }
}

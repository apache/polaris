/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.gradle.api.plugins.jvm.JvmTestSuite
import org.gradle.api.tasks.Input
import org.gradle.kotlin.dsl.register
import org.gradle.kotlin.dsl.withType
import org.gradle.process.CommandLineArgumentProvider

class QuarkusProfileArgumentProvider(private val profile: String) : CommandLineArgumentProvider {
  @get:Input
  val inputProfile: String
    get() = profile

  override fun asArguments(): Iterable<String> = listOf("-Dquarkus.profile=$profile")
}

plugins { id("polaris-server") }

testing {
  suites {
    withType<JvmTestSuite> {
      targets.configureEach {
        testTask.configure {
          systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
          // Enable automatic extension detection to execute GradleDuplicateLoggingWorkaround
          // automatically.
          // See https://github.com/quarkusio/quarkus/issues/22844
          systemProperty("junit.jupiter.extensions.autodetection.enabled", "true")
        }
      }
    }
    fun intTestSuiteConfigure(testSuite: JvmTestSuite) = testSuite.run {
      targets.configureEach {
        testTask.configure {
          // For Quarkus...
          //
          // io.quarkus.test.junit.IntegrationTestUtil.determineBuildOutputDirectory(java.net.URL)
          // is not smart enough :(
          systemProperty("build.output.directory", layout.buildDirectory.asFile.get())
          dependsOn(tasks.named("quarkusBuild"))
          // Set the 'it' profile explicitly
          jvmArgumentProviders.add(QuarkusProfileArgumentProvider("it"))
        }
      }
      tasks.named(sources.compileJavaTaskName).configure {
        dependsOn("compileQuarkusTestGeneratedSourcesJava")
      }
      configurations.named(sources.runtimeOnlyConfigurationName).configure {
        extendsFrom(configurations.named("testRuntimeOnly"))
      }
      configurations.named(sources.implementationConfigurationName).configure {
        // Let the test's implementation config extend testImplementation, so it also inherits the
        // project's "main" implementation dependencies (not just the "api" configuration)
        extendsFrom(configurations.named("testImplementation"))
      }
      sources { java.srcDirs(tasks.named("quarkusGenerateCodeTests")) }
    }

    listOf("intTest", "cloudTest").forEach {
      register<JvmTestSuite>(it).configure { intTestSuiteConfigure(this) }
    }
  }
}

dependencies {
  // All Quarkus projects should use JBoss LogManager with SLF4J, instead of Logback
  implementation("org.jboss.slf4j:slf4j-jboss-logmanager")
}

configurations.configureEach {
  // Validate that Logback dependencies are not used in Quarkus modules.
  dependencies.configureEach {
    if (group == "ch.qos.logback") {
      throw GradleException(
        "Logback dependencies are not allowed in Quarkus modules. " +
          "Found $group:$name in ${project.name}."
      )
    }
  }
}

configurations.named("intTestRuntimeOnly").configure {
  extendsFrom(configurations.named("testRuntimeOnly"))
}

tasks.named("compileJava") { dependsOn("compileQuarkusGeneratedSourcesJava") }

tasks.named("sourcesJar") { dependsOn("compileQuarkusGeneratedSourcesJava") }

tasks.named("javadoc") { dependsOn("jandex") }

tasks.named("quarkusDependenciesBuild") { dependsOn("jandex") }

tasks.named("imageBuild") { dependsOn("jandex") }

tasks.withType(Test::class.java).configureEach {
  // Gradle's Jacoco plugin doesn't work well with Quarkus's test coverage
  extensions.configure(JacocoTaskExtension::class) { isEnabled = false }

  maxParallelForks = 1

  // Silence the 'OpenJDK 64-Bit Server VM warning: Sharing is only supported for boot loader
  // classes because bootstrap classpath has been appended' warning from OpenJDK.
  jvmArgs("-Xshare:off")
}

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

plugins { checkstyle }

val checkStyleVersionValue =
  versionCatalogs
    .named("libs")
    .findVersion("checkstyle")
    .orElseThrow { GradleException("checkstyle not declared in libs.versions.toml") }
    .requiredVersion

checkstyle {
  toolVersion = checkStyleVersionValue
  config = MemoizedCheckstyleConfig.checkstyleConfig(rootProject)
  isShowViolations = true
  isIgnoreFailures = false
}

configurations.configureEach {
  // Avoids dependency resolution error:
  // Could not resolve all task dependencies for configuration '...:checkstyle'.
  //   > Module 'com.google.guava:guava' has been rejected:
  //     Cannot select module with conflict on capability
  //         'com.google.collections:google-collections:33.0.0-jre'
  //         also provided by [com.google.collections:google-collections:1.0(runtime)]
  //   > Module 'com.google.collections:google-collections' has been rejected:
  //     Cannot select module with conflict on capability
  //         'com.google.collections:google-collections:1.0'
  //         also provided by [com.google.guava:guava:33.0.0-jre(jreRuntimeElements)]
  resolutionStrategy.capabilitiesResolution.withCapability(
    "com.google.collections:google-collections"
  ) {
    selectHighestVersion()
  }
}

tasks.withType<Checkstyle>().configureEach {
  if (plugins.hasPlugin("org.kordamp.gradle.jandex")) {
    when (name) {
      "checkstyleMain" -> {
        dependsOn("jandex")
        inputs.files(tasks.named("jandex").get().outputs.files)
      }
      else -> {}
    }
  }

  if (plugins.hasPlugin("io.quarkus")) {
    when (name) {
      "checkstyleQuarkusGeneratedSources" -> {
        dependsOn("quarkusGenerateCode")
        dependsOn("quarkusGenerateCodeDev")
      }
      "checkstyleQuarkusTestGeneratedSources" -> {
        dependsOn("quarkusGenerateCodeTests")
      }
      else -> {}
    }
  }

  maxWarnings = 0 // treats warnings as errors
  setSource(fileTree("src/main/java") { exclude("**/generated/**") })
}

val sourceSets = project.extensions.findByType(SourceSetContainer::class.java)

if (sourceSets != null) {
  val checkstyleAll = tasks.register("checkstyle")
  checkstyleAll.configure { description = "Checkstyle all source sets" }

  sourceSets.withType(SourceSet::class.java).configureEach {
    val sourceSet = this
    val checkstyleTask = tasks.named(sourceSet.getTaskName("checkstyle", null))
    checkstyleAll.configure { dependsOn(checkstyleTask) }
  }
}

private class MemoizedCheckstyleConfig {
  companion object {
    fun checkstyleConfig(rootProject: Project): TextResource {
      val e = rootProject.extensions.getByType(ExtraPropertiesExtension::class)
      if (e.has("polaris-checkstyle-config")) {
        return e.get("polaris-checkstyle-config") as TextResource
      }
      val configResource = rootProject.resources.text.fromFile("codestyle/checkstyle-config.xml")
      e.set("polaris-checkstyle-config", configResource)
      return configResource
    }
  }
}

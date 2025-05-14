/*
 * Copyright (C) 2023 Dremio
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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins { id("com.gradleup.shadow") }

val shadowJar = tasks.named<ShadowJar>("shadowJar")

shadowJar.configure {
  outputs.cacheIf { false } // do not cache uber/shaded jars
  archiveClassifier = ""
  mergeServiceFiles()
}

tasks.named<Jar>("jar").configure { archiveClassifier = "raw" }

tasks.withType<ShadowJar>().configureEach {
  exclude("META-INF/jandex.idx")
  isZip64 = true
}

tasks.named("assemble").configure { dependsOn("shadowJar") }

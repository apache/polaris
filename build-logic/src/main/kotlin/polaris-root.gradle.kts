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

import org.jetbrains.gradle.ext.copyright
import org.jetbrains.gradle.ext.encodings
import org.jetbrains.gradle.ext.settings
import publishing.PublishingHelperExtension
import publishing.PublishingHelperPlugin

plugins {
  id("com.diffplug.spotless")
  id("org.jetbrains.gradle.plugin.idea-ext")
}

apply<PublishingHelperPlugin>()

spotless {
  kotlinGradle {
    ktfmt().googleStyle()
    licenseHeaderFile(rootProject.file("codestyle/copyright-header-java.txt"), "$")
    target("*.gradle.kts", "build-logic/*.gradle.kts", "build-logic/src/**/*.kt*")
  }
}

if (System.getProperty("idea.sync.active").toBoolean()) {
  idea {
    module {
      isDownloadJavadoc = false // was 'true', but didn't work
      isDownloadSources = false // was 'true', but didn't work
      inheritOutputDirs = true
    }

    project.settings {
      copyright {
        useDefault = "ApacheLicense-v2"
        profiles.create("ApacheLicense-v2") {
          // strip trailing LF
          val copyrightText = rootProject.file("codestyle/copyright-header.txt").readText()
          notice = copyrightText
        }
      }

      encodings.encoding = "UTF-8"
      encodings.properties.encoding = "UTF-8"
    }
  }
}

extensions.getByType<PublishingHelperExtension>().apply {
  asfProjectName = "polaris"

  mailingLists.addAll("dev", "issues", "commits")

  podlingPpmcAsfIds.addAll(
    "anoop",
    "ashvin",
    "jackye",
    "jbonofre",
    "russellspitzer",
    "snazy",
    "takidau",
    "vvcephei"
  )
  podlingMentorsAsfIds.addAll("bdelacretaz", "blue", "holden", "jbonofre", "yao")
  podlingCommitterAsfIds.addAll("adutra", "annafil", "emaynard", "collado", "yufei", "ebyhr")
}

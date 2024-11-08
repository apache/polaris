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

package publishing

import java.io.File
import javax.inject.Inject
import org.gradle.api.Project
import org.gradle.api.model.ObjectFactory
import org.gradle.kotlin.dsl.listProperty
import org.gradle.kotlin.dsl.property

/**
 * Gradle plugin extension object for the `PublishingHelperPlugin. Most attributes are likely never
 * changed from the default values.
 *
 * Apache podlings need to specify the PPMC members and committers manually, Apache TLPs don't
 * populate these properties.
 */
abstract class PublishingHelperExtension
@Inject
constructor(objectFactory: ObjectFactory, project: Project) {
  // optional customization of the pom.xml <name> element
  val mavenName = objectFactory.property<String>().convention(project.provider { project.name })

  val licenseUrl =
    objectFactory.property<String>().convention("https://www.apache.org/licenses/LICENSE-2.0.txt")

  // the following are only relevant on the root project
  val asfProjectName = objectFactory.property<String>()
  val baseName =
    objectFactory
      .property<String>()
      .convention(project.provider { "apache-${asfProjectName.get()}-${project.version}" })
  val distributionDir =
    objectFactory.directoryProperty().convention(project.layout.buildDirectory.dir("distributions"))
  val sourceTarball =
    objectFactory
      .fileProperty()
      .convention(project.provider { distributionDir.get().file("${baseName.get()}.tar.gz") })
  val sourceTarballDigest =
    objectFactory
      .fileProperty()
      .convention(project.provider { distributionDir.get().file("${baseName.get()}.sha512") })

  val mailingLists = objectFactory.listProperty(String::class.java).convention(emptyList())

  // override the list of developers (P)PMC members + committers, necessary for podlings
  val podlingPpmcAsfIds = objectFactory.setProperty(String::class.java).convention(emptySet())
  val podlingMentorsAsfIds = objectFactory.setProperty(String::class.java).convention(emptySet())
  val podlingCommitterAsfIds = objectFactory.setProperty(String::class.java).convention(emptySet())

  fun distributionFile(ext: String): File =
    distributionDir.get().file("${baseName.get()}.$ext").asFile
}

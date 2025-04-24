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
  // the following are only relevant on the root project

  /**
   * Lowercase ASF project ID, as present in keys in the JSON docs describing the projects (for
   * example in `https://whimsy.apache.org/public/public_ldap_projects.json`).
   */
  val asfProjectId = objectFactory.property<String>().convention(project.name)

  /** Used to override the full project name, for example `Apache Polaris`. */
  val overrideName = objectFactory.property<String>()
  /** Used to override the project description as it appears in published Maven poms. */
  val overrideDescription = objectFactory.property<String>()
  /** Used to override the project URL as it appears in published Maven poms. */
  val overrideProjectUrl = objectFactory.property<String>()
  /**
   * Used to override the name of the GitHub repo in the apache organization. Defaults to the
   * project ID.
   */
  val githubRepositoryName = objectFactory.property<String>()
  /**
   * Used to override the project's SCM as it appears in published Maven poms. Default is derived
   * from `githubRepoName`.
   */
  val overrideScm = objectFactory.property<String>()
  /** Used to override the project's issue management URL as it appears in published Maven poms. */
  val overrideIssueManagement = objectFactory.property<String>()
  /** Prefix for the tag published for non-SNAPSHOT versions in the Maven poms. */
  val overrideTagPrefix = objectFactory.property<String>()

  /** The published distributables, including the source tarball, base file name. */
  val baseName =
    objectFactory
      .property<String>()
      .convention(project.provider { "apache-${asfProjectId.get()}-${project.version}" })

  val distributionDir =
    objectFactory.directoryProperty().convention(project.layout.buildDirectory.dir("distributions"))
  val sourceTarball =
    objectFactory
      .fileProperty()
      .convention(project.provider { distributionDir.get().file("${baseName.get()}.tar.gz") })

  /** List of mailing-lists. */
  val mailingLists = objectFactory.listProperty(String::class.java).convention(emptyList())

  fun distributionFile(ext: String): File =
    distributionDir.get().file("${baseName.get()}.$ext").asFile
}

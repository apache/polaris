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

import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.scala.ScalaCompile
import org.gradle.api.tasks.scala.ScalaDoc
import org.gradle.kotlin.dsl.*
import org.gradle.language.scala.tasks.KeepAliveMode

plugins {
  id("polaris-server")
  scala
}

tasks.withType<ScalaCompile>().configureEach {
  options.release = 21
  scalaCompileOptions.additionalParameters.add("-release:21")
  sourceCompatibility = "21"
  targetCompatibility = "21"
}

tasks.withType<ScalaCompile>().configureEach {
  scalaCompileOptions.keepAliveMode = KeepAliveMode.DAEMON
  scalaCompileOptions.encoding = "UTF-8"
}

val scaladoc = tasks.named<ScalaDoc>("scaladoc")
val scaladocJar = tasks.register<Jar>("scaladocJar")

scaladocJar.configure {
  dependsOn(scaladoc)
  val baseJar = tasks.getByName<Jar>("jar")
  from(scaladoc.get().destinationDir)
  destinationDirectory = baseJar.destinationDirectory
  archiveClassifier = "scaladoc"
}

tasks.named("assemble").configure { dependsOn(scaladocJar) }

configure<PublishingExtension> {
  publications {
    withType(MavenPublication::class.java) {
      if (name == "maven") {
        artifact(scaladocJar)
      }
    }
  }
}

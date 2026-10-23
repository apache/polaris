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

package org.apache.polaris.gradle.sbom

import org.apache.polaris.gradle.sbom.PackageKind.SHADED_JAR
import org.cyclonedx.gradle.CyclonedxDirectTask
import org.cyclonedx.gradle.CyclonedxPlugin
import org.gradle.api.Project
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.RegularFile
import org.gradle.api.plugins.ExtraPropertiesExtension
import org.gradle.api.provider.Provider
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.Sync
import org.gradle.api.tasks.bundling.AbstractArchiveTask
import org.gradle.kotlin.dsl.extra
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.register
import org.gradle.kotlin.dsl.withType
import org.gradle.plugins.signing.Sign

/** Polaris producer and publication wiring; keep separate. */
@Suppress("unused")
fun Project.configureDistributionSbom() {
  // The producers own SBOM generation. One finalizer supplies the final artifact
  // identity/selection.
  val subject = SbomSubject(group.toString(), name, version.toString(), SHADED_JAR, "bundle")
  pluginManager.apply(CyclonedxPlugin::class.java)
  tasks.named<CyclonedxDirectTask>("cyclonedxDirectBom") {
    includeConfigs.set(listOf("runtimeClasspath"))
    includeBomSerialNumber.set(false)
    includeBuildSystem.set(false)
    xmlOutput.unsetConvention()
  }

  val shadowInputs = extra

  val finalSbom =
    tasks.register<FinalizeDistributionSbom>("finalizeDistributionSbom") {
      group = "build"
      description = "Adds final subject identity and actual assembly inputs to the producer SBOM"
      this.subject.set(subject)
      dependsOn("createPolarisSparkJar")
      val sbomShadow = SbomShadow.sbomShadowFromExtra(shadowInputs)
      subjectFile.set(sbomShadow.archive)
      selectedFiles.from(sbomShadow.selected)
      metadataByPath.set(sbomShadow.metadata)
      minimized.set(sbomShadow.minimized)
      producer.set(tasks.named<CyclonedxDirectTask>("cyclonedxDirectBom").flatMap { it.jsonOutput })
      document.set(layout.buildDirectory.file("reports/sbom/${subject.sbomFilename}"))
    }

  val signingTasks = tasks.withType<Sign>()
  val signatures = providers.provider {
    val document = finalSbom.get().document.get().asFile
    signingTasks.flatMap { sign ->
      sign.signatures.filter { it.toSign == document }.map { it.file }
    }
  }
  val digests = SbomShadow.sbomShadowFromExtra(shadowInputs).digests
  tasks.register<Sync>("stageSparkSboms") {
    group = "distribution"
    description = "Stages this Spark publication's SBOM, signature and SHA-512 checksum"
    dependsOn(signingTasks)
    from(finalSbom.flatMap { it.document })
    from(signatures)
    from(digests)
    into(rootProject.layout.buildDirectory.dir("spark-sboms/${project.name}"))
  }

  afterEvaluate {
    // Realize publications outside lazy publication actions.
    extensions.getByType<PublishingExtension>().publications.withType<MavenPublication>().forEach {
      publication ->
      publication.artifact(finalSbom.flatMap { it.document }) {
        classifier = subject.sbomClassifier
        extension = "json"
      }
    }
  }
}

private class SbomShadow(
  val archive: Provider<RegularFile>,
  val selected: Provider<ConfigurableFileCollection>,
  val metadata: Provider<Map<String, Map<String, String>>>,
  val minimized: Provider<Boolean>,
  val digests: Provider<ConfigurableFileCollection>,
) {
  companion object {
    @Suppress("UNCHECKED_CAST")
    fun sbomShadowFromExtra(extra: ExtraPropertiesExtension): SbomShadow {
      // Produced in build-logic/src/main/kotlin/polaris-spark-license-checks.gradle.kts
      val sbomShadow = extra["sbomShadow"] as Map<String, Any>
      return SbomShadow(
        sbomShadow["archive"] as Provider<RegularFile>,
        sbomShadow["selected"] as Provider<ConfigurableFileCollection>,
        sbomShadow["metadata"] as Provider<Map<String, Map<String, String>>>,
        sbomShadow["minimized"] as Provider<Boolean>,
        sbomShadow["digests"] as Provider<ConfigurableFileCollection>,
      )
    }
  }
}

/** Binary archive sidecars are explicit release inputs; ordinary check stays license-only. */
fun Project.configureBinaryArchiveSboms() {
  val documents =
    listOf("Zip" to PackageKind.BINARY_ZIP, "Tar" to PackageKind.BINARY_TAR).map {
      (kind, packageKind) ->
      val archive = tasks.named<AbstractArchiveTask>("dist$kind")
      tasks.register<FinalizeBinaryArchiveSbom>("finalizeDist${kind}Sbom") {
        group = "build"
        description = "Finalizes the SBOM for the actual binary $kind archive"
        dependsOn("checkDistributionLicenses")
        this.archive.set(archive.flatMap { it.archiveFile })
        adminTree.fileProvider(
          configurations.getByName("adminDistribution").elements.map { it.single().asFile },
        )
        serverTree.fileProvider(
          configurations.getByName("serverDistribution").elements.map { it.single().asFile },
        )
        adminProducer.fileProvider(
          configurations.getByName("adminSbom").elements.map { it.single().asFile },
        )
        serverProducer.fileProvider(
          configurations.getByName("serverSbom").elements.map { it.single().asFile },
        )
        subject.set(
          SbomSubject(
            project.group.toString(),
            "polaris-bin",
            project.version.toString(),
            packageKind,
          ),
        )
        document.set(
          layout.buildDirectory.file(
            "distributions/polaris-bin-${project.version}${packageKind.sbomBasenameSuffix}",
          ),
        )
      }
    }
  tasks.register("assembleDistributionSboms") {
    group = "build"
    description = "Produces SBOM sidecars for binary ZIP and TAR distributions"
    dependsOn(documents)
  }
}

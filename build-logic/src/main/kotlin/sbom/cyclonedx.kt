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

package sbom

import GitInfo
import java.util.UUID
import java.util.function.Consumer
import javax.inject.Inject
import org.cyclonedx.gradle.BaseCyclonedxTask
import org.cyclonedx.gradle.CyclonedxAggregateTask
import org.cyclonedx.gradle.CyclonedxDirectTask
import org.cyclonedx.gradle.CyclonedxPlugin
import org.cyclonedx.gradle.utils.CyclonedxUtils
import org.cyclonedx.model.Bom
import org.cyclonedx.model.Component
import org.cyclonedx.model.Component.Type.APPLICATION
import org.cyclonedx.model.Dependency
import org.cyclonedx.model.ExtensibleElement
import org.cyclonedx.model.Extension
import org.cyclonedx.model.ExternalReference
import org.cyclonedx.model.License
import org.cyclonedx.model.LicenseChoice
import org.cyclonedx.model.Metadata
import org.cyclonedx.model.OrganizationalEntity
import org.cyclonedx.parsers.BomParserFactory
import org.gradle.api.Project
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.model.ObjectFactory
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.TaskProvider
import org.gradle.jvm.tasks.Jar
import org.gradle.kotlin.dsl.attributes
import org.gradle.kotlin.dsl.named
import publishing.EffectiveAsfProject

/**
 * Configures [CycloneDX plugin tasks](https://github.com/CycloneDX/cyclonedx-gradle-plugin) for the
 * Apache Polaris project.
 *
 * The `cyclonedxBom` task, only available on the root Gradle project, generates an aggregated BOM
 * for the project and all its modules.
 *
 * The `cyclonedxDirectBom` task generates the SBOM for the projects' main artifacts. The SBOM for
 * each published Maven artifact uses the classifier `cyclonedx` and is published as JSON and XML.
 */
fun Project.configureCycloneDx() {
  tasks.withType(BaseCyclonedxTask::class.java).configureEach { configureAllForPolaris() }

  val cyclonedxBom = tasks.named<CyclonedxAggregateTask>("cyclonedxBom")
  cyclonedxBom.configure {
    if (project != rootProject) {
      // Disable aggregation for subprojects
      enabled = false
    }
    group = "publishing"
    description = "Aggregate CycloneDX SBOMs from all subprojects"
  }

  val cyclonedxDirectBom = tasks.named<CyclonedxDirectTask>("cyclonedxDirectBom")
  cyclonedxDirectBom.configure {
    includeConfigs.addAll("runtimeClasspath")
    group = "publishing"
    description = "Generate CycloneDX SBOMs for this as a library"
  }

  createCyclonedxConfigurations(this, cyclonedxBom)
  createCyclonedxConfigurations(this, cyclonedxDirectBom)

  extensions.getByType(PublishingExtension::class.java).publications {
    named<MavenPublication>("maven") {
      artifact(cyclonedxDirectBom.map { t -> t.jsonOutput }) {
        classifier = "cyclonedx"
        builtBy(cyclonedxDirectBom)
      }
      artifact(cyclonedxDirectBom.map { t -> t.xmlOutput }) {
        classifier = "cyclonedx"
        builtBy(cyclonedxDirectBom)
      }
    }
  }

  if (plugins.hasPlugin("java-library")) {
    tasks.named<Jar>("jar") {
      manifest {
        attributes(
          // TODO these attributes are made up and do not adhere to any standard nor to any
          //  de-facto standard
          "CycloneDX-JSON" to
            "in-jar:META-INF/maven/${project.group}/${project.name}/cyclonedx.json",
          "CycloneDX-XML" to "in-jar:META-INF/maven/${project.group}/${project.name}/cyclonedx.xml",
          "License-SPDX" to "Apache-2.0",
          "License-Text" to "in-jar:META-INF/LICENSE",
          "Notice-Text" to "in-jar:META-INF/NOTICE",
        )
      }
      from(cyclonedxDirectBom) {
        eachFile {
          // TODO the SBOM location is made up and do not adhere to any standard nor to any
          //  de-facto standard
          path =
            Regex("^bom[.](xml|json)$")
              .replace(name, "META-INF/maven/${project.group}/${project.name}/cyclonedx.$1")
        }
      }
    }
  }
}

fun <T : BaseCyclonedxTask> createCyclonedxConfigurations(
  project: Project,
  cdxTask: TaskProvider<T>,
) {
  val baseName = cdxTask.name
  val cfgAll = project.configurations.create("${baseName}All")
  val cfgJson = project.configurations.create("${baseName}Json")
  val cfgXml = project.configurations.create("${baseName}Xml")
  project.artifacts.add(cfgAll.name, cdxTask.map { t -> t.jsonOutput }) { builtBy(cdxTask) }
  project.artifacts.add(cfgAll.name, cdxTask.map { t -> t.xmlOutput }) { builtBy(cdxTask) }
  project.artifacts.add(cfgJson.name, cdxTask.map { t -> t.jsonOutput }) { builtBy(cdxTask) }
  project.artifacts.add(cfgXml.name, cdxTask.map { t -> t.xmlOutput }) { builtBy(cdxTask) }
  for (c in listOf(cfgAll, cfgJson, cfgXml)) {
    c.isCanBeConsumed = true
    c.isCanBeResolved = true
  }
}

internal fun BaseCyclonedxTask.configureAllForPolaris() {
  // Needed for projects that use subdirectories in their build/ directory, like the Spark plugin
  jsonOutput.set(project.layout.buildDirectory.file("reports/cyclonedx/bom.json"))
  xmlOutput.set(project.layout.buildDirectory.file("reports/cyclonedx/bom.xml"))

  val prj = EffectiveAsfProject.forProject(project)
  val gitInfo = GitInfo.memoized(project)

  organizationalEntity.set(
    OrganizationalEntity().apply {
      name = prj.fullName().get()
      urls = listOf(prj.projectUrl().get())
    }
  )
  externalReferences.add(
    ExternalReference().apply {
      type = ExternalReference.Type.VCS
      url = prj.codeRepoUrl().get() + ".git"
    }
  )
  externalReferences.add(
    ExternalReference().apply {
      type = ExternalReference.Type.WEBSITE
      url = prj.projectUrl().get()
    }
  )
  externalReferences.add(
    ExternalReference().apply {
      type = ExternalReference.Type.ISSUE_TRACKER
      url = prj.issueTracker().get()
    }
  )
  externalReferences.add(
    ExternalReference().apply {
      type = ExternalReference.Type.MAILING_LIST
      url = prj.mailingList("dev").archive()
    }
  )
  externalReferences.add(
    ExternalReference().apply {
      type = ExternalReference.Type.SECURITY_CONTACT
      url = "mailto:security@apache.org"
    }
  )
  licenseChoice.convention(
    LicenseChoice().apply {
      addLicense(
        License().apply {
          id = "Apache-2.0"
          url = gitInfo.rawGithubLink("LICENSE")
        }
      )
    }
  )
}

@CacheableTask
abstract class CyclonedxBundleTask @Inject constructor(objectFactory: ObjectFactory) :
  BaseCyclonedxTask() {
  init {
    group = "publishing"
    description = "Generate CycloneDX SBOM for the distribution bundle"

    jsonOutput.set(project.layout.buildDirectory.file("reports/cyclonedx-bundle/bom.json"))
    xmlOutput.set(project.layout.buildDirectory.file("reports/cyclonedx-bundle/bom.xml"))
  }

  @get:InputFiles
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val inputBoms: ConfigurableFileCollection

  @get:Input val includeDependencies = objectFactory.property(Boolean::class.java).convention(true)

  @TaskAction
  fun aggregate() {
    val bom: Bom = buildBom()
    logger.info("{} Writing BOM", CyclonedxPlugin.LOG_PREFIX)
    if (jsonOutput.isPresent) {
      CyclonedxUtils.writeJsonBom(schemaVersion.get(), bom, jsonOutput.asFile.get())
    }
    if (xmlOutput.isPresent) {
      CyclonedxUtils.writeXmlBom(schemaVersion.get(), bom, xmlOutput.asFile.get())
    }
  }

  private fun buildBom(): Bom {
    val rootComponent = buildRootComponent()

    val seenBomRefs = mutableSetOf<String>()
    val allDependencies = mutableMapOf<String, Dependency>()

    val bom = Bom()
    bom.metadata = buildMetadata(rootComponent)

    logger.info("{} Reading BOMs", CyclonedxPlugin.LOG_PREFIX)
    inputBoms
      .map { file ->
        logger.info("{} Reading BOM from {}", CyclonedxPlugin.LOG_PREFIX, file)
        BomParserFactory.createParser(file).parse(file)
      }
      .forEach {
        val metadata = it.metadata
        // Add a component from the source SBOM's root component, with metadata information
        val appComponent = createComponent(metadata)
        if (bom.components == null) {
          bom.addComponent(appComponent)
        } else {
          // This is to keep the included application components at the beginning, no technical
          // reason for this
          bom.components.add(0, appComponent)
        }
        // Add missing components
        it.components?.forEach { c ->
          if (seenBomRefs.add(c.bomRef)) {
            bom.addComponent(c)
          }
        }
        // Add dependencies
        it.dependencies?.forEach { d -> allDependencies[d.ref] = d }
      }

    if (includeBomSerialNumber.get()) {
      bom.serialNumber = "urn:uuid:${UUID.randomUUID()}"
    }
    if (includeDependencies.get()) {
      bom.dependencies = allDependencies.values.toMutableList()
    }
    return bom
  }

  private fun buildMetadata(rootComponent: Component): Metadata {
    val metadata = Metadata()
    if (licenseChoice.isPresent) {
      metadata.licenses = licenseChoice.get()
    }
    if (organizationalEntity.isPresent && (OrganizationalEntity() != organizationalEntity.get())) {
      metadata.manufacturer = organizationalEntity.get()
    }
    metadata.component = rootComponent
    return metadata
  }

  private fun buildRootComponent(): Component {
    val t = this
    return Component().apply {
      type = APPLICATION
      name = t.componentName.get()
      version = t.componentVersion.get()
      if (t.externalReferences.isPresent) {
        t.externalReferences
          .get()
          .forEach(
            Consumer { externalReference: ExternalReference ->
              addExternalReference(externalReference)
            }
          )
      }
    }
  }

  private fun createComponent(metadata: Metadata): Component =
    Component().apply {
      val src = metadata.component
      this.bomRef = src.bomRef
      this.mimeType = src.mimeType
      this.type = src.type
      this.supplier = src.supplier
      this.author = src.author
      this.publisher = src.publisher
      this.group = src.group
      this.name = src.name
      this.version = src.version
      this.description = src.description
      this.scope = src.scope
      this.hashes = src.hashes
      this.licenses = src.licenses
      this.copyright = src.copyright
      this.cpe = src.cpe
      this.purl = src.purl
      this.omniborId = src.omniborId
      this.swhid = src.swhid
      this.swid = src.swid
      this.modified = src.modified
      this.pedigree = src.pedigree
      this.externalReferences = src.externalReferences
      this.properties = src.properties
      this.components = src.components
      this.evidence = src.evidence
      this.releaseNotes = src.releaseNotes
      this.modelCard = src.modelCard
      this.data = src.data
      this.cryptoProperties = src.cryptoProperties
      this.provides = src.provides
      this.tags = src.tags
      this.authors = src.authors
      this.manufacturer = src.manufacturer
      this.signature = src.signature
      cloneExtElem(src, this)

      this.licenses = metadata.licenses
      this.manufacturer = metadata.manufacturer
      metadata.properties?.forEach { p -> this.addProperty(p) }
      this.authors = metadata.authors
      this.supplier = metadata.supplier
    }

  private fun cloneExtElem(src: ExtensibleElement, dst: ExtensibleElement) {
    src.extensions?.forEach { (key, ext) -> dst.add(key, cloneExtension(ext)) }
    src.extensibleTypes?.forEach { e -> dst.extensibleTypes.add(e) }
  }

  private fun cloneExtension(src: Extension): Extension =
    Extension(src.extensionType, src.extensions).apply {
      this.prefix = src.prefix
      this.namespaceURI = src.namespaceURI
    }
}

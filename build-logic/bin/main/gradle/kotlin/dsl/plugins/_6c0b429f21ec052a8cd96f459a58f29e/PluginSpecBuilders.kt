/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

@file:Suppress(
    "unused",
    "nothing_to_inline",
    "useless_cast",
    "unchecked_cast",
    "extension_shadowed_by_member",
    "redundant_projection",
    "RemoveRedundantBackticks",
    "ObjectPropertyName",
    "deprecation",
    "detekt:all"
)
@file:org.gradle.api.Generated

package gradle.kotlin.dsl.plugins._6c0b429f21ec052a8cd96f459a58f29e

import org.gradle.plugin.use.PluginDependenciesSpec
import org.gradle.plugin.use.PluginDependencySpec


/**
 * The `com` plugin group.
 */
@org.gradle.api.Generated
internal
class `ComPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `com`.
 */
internal
val `PluginDependenciesSpec`.`com`: `ComPluginGroup`
    get() = `ComPluginGroup`(this)


/**
 * The `com.diffplug` plugin group.
 */
@org.gradle.api.Generated
internal
class `ComDiffplugPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `com.diffplug`.
 */
internal
val `ComPluginGroup`.`diffplug`: `ComDiffplugPluginGroup`
    get() = `ComDiffplugPluginGroup`(plugins)


/**
 * The `com.diffplug.gradle` plugin group.
 */
@org.gradle.api.Generated
internal
class `ComDiffplugGradlePluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `com.diffplug.gradle`.
 */
internal
val `ComDiffplugPluginGroup`.`gradle`: `ComDiffplugGradlePluginGroup`
    get() = `ComDiffplugGradlePluginGroup`(plugins)


/**
 * The `com.diffplug.gradle.spotless` plugin implemented by [com.diffplug.gradle.spotless.SpotlessPluginRedirect].
 */
internal
val `ComDiffplugGradlePluginGroup`.`spotless`: PluginDependencySpec
    get() = plugins.id("com.diffplug.gradle.spotless")


/**
 * The `com.diffplug.spotless` plugin implemented by [com.diffplug.gradle.spotless.SpotlessPlugin].
 */
internal
val `ComDiffplugPluginGroup`.`spotless`: PluginDependencySpec
    get() = plugins.id("com.diffplug.spotless")


/**
 * The `com.github` plugin group.
 */
@org.gradle.api.Generated
internal
class `ComGithubPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `com.github`.
 */
internal
val `ComPluginGroup`.`github`: `ComGithubPluginGroup`
    get() = `ComGithubPluginGroup`(plugins)


/**
 * The `com.github.jk1` plugin group.
 */
@org.gradle.api.Generated
internal
class `ComGithubJk1PluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `com.github.jk1`.
 */
internal
val `ComGithubPluginGroup`.`jk1`: `ComGithubJk1PluginGroup`
    get() = `ComGithubJk1PluginGroup`(plugins)


/**
 * The `com.github.jk1.dependency-license-report` plugin implemented by [com.github.jk1.license.LicenseReportPlugin].
 */
internal
val `ComGithubJk1PluginGroup`.`dependency-license-report`: PluginDependencySpec
    get() = plugins.id("com.github.jk1.dependency-license-report")


/**
 * The `com.github.johnrengelman` plugin group.
 */
@org.gradle.api.Generated
internal
class `ComGithubJohnrengelmanPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `com.github.johnrengelman`.
 */
internal
val `ComGithubPluginGroup`.`johnrengelman`: `ComGithubJohnrengelmanPluginGroup`
    get() = `ComGithubJohnrengelmanPluginGroup`(plugins)


/**
 * The `com.github.johnrengelman.shadow` plugin implemented by [com.github.jengelman.gradle.plugins.shadow.legacy.LegacyShadowPlugin].
 */
internal
val `ComGithubJohnrengelmanPluginGroup`.`shadow`: PluginDependencySpec
    get() = plugins.id("com.github.johnrengelman.shadow")


/**
 * The `com.gradleup` plugin group.
 */
@org.gradle.api.Generated
internal
class `ComGradleupPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `com.gradleup`.
 */
internal
val `ComPluginGroup`.`gradleup`: `ComGradleupPluginGroup`
    get() = `ComGradleupPluginGroup`(plugins)


/**
 * The `com.gradleup.shadow` plugin implemented by [com.github.jengelman.gradle.plugins.shadow.ShadowPlugin].
 */
internal
val `ComGradleupPluginGroup`.`shadow`: PluginDependencySpec
    get() = plugins.id("com.gradleup.shadow")


/**
 * The `io` plugin group.
 */
@org.gradle.api.Generated
internal
class `IoPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `io`.
 */
internal
val `PluginDependenciesSpec`.`io`: `IoPluginGroup`
    get() = `IoPluginGroup`(this)


/**
 * The `io.github` plugin group.
 */
@org.gradle.api.Generated
internal
class `IoGithubPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `io.github`.
 */
internal
val `IoPluginGroup`.`github`: `IoGithubPluginGroup`
    get() = `IoGithubPluginGroup`(plugins)


/**
 * The `io.github.gradle-nexus` plugin group.
 */
@org.gradle.api.Generated
internal
class `IoGithubGradle-nexusPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `io.github.gradle-nexus`.
 */
internal
val `IoGithubPluginGroup`.`gradle-nexus`: `IoGithubGradle-nexusPluginGroup`
    get() = `IoGithubGradle-nexusPluginGroup`(plugins)


/**
 * The `io.github.gradle-nexus.publish-plugin` plugin implemented by [io.github.gradlenexus.publishplugin.NexusPublishPlugin].
 */
internal
val `IoGithubGradle-nexusPluginGroup`.`publish-plugin`: PluginDependencySpec
    get() = plugins.id("io.github.gradle-nexus.publish-plugin")


/**
 * The `net` plugin group.
 */
@org.gradle.api.Generated
internal
class `NetPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `net`.
 */
internal
val `PluginDependenciesSpec`.`net`: `NetPluginGroup`
    get() = `NetPluginGroup`(this)


/**
 * The `net.ltgt` plugin group.
 */
@org.gradle.api.Generated
internal
class `NetLtgtPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `net.ltgt`.
 */
internal
val `NetPluginGroup`.`ltgt`: `NetLtgtPluginGroup`
    get() = `NetLtgtPluginGroup`(plugins)


/**
 * The `net.ltgt.errorprone` plugin implemented by [net.ltgt.gradle.errorprone.ErrorPronePlugin].
 */
internal
val `NetLtgtPluginGroup`.`errorprone`: PluginDependencySpec
    get() = plugins.id("net.ltgt.errorprone")


/**
 * The `org` plugin group.
 */
@org.gradle.api.Generated
internal
class `OrgPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `org`.
 */
internal
val `PluginDependenciesSpec`.`org`: `OrgPluginGroup`
    get() = `OrgPluginGroup`(this)


/**
 * The `org.gradle` plugin group.
 */
@org.gradle.api.Generated
internal
class `OrgGradlePluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `org.gradle`.
 */
internal
val `OrgPluginGroup`.`gradle`: `OrgGradlePluginGroup`
    get() = `OrgGradlePluginGroup`(plugins)


/**
 * The `org.gradle.antlr` plugin implemented by [org.gradle.api.plugins.antlr.AntlrPlugin].
 */
internal
val `OrgGradlePluginGroup`.`antlr`: PluginDependencySpec
    get() = plugins.id("org.gradle.antlr")


/**
 * The `org.gradle.application` plugin implemented by [org.gradle.api.plugins.ApplicationPlugin].
 */
internal
val `OrgGradlePluginGroup`.`application`: PluginDependencySpec
    get() = plugins.id("org.gradle.application")


/**
 * The `org.gradle.assembler` plugin implemented by [org.gradle.language.assembler.plugins.AssemblerPlugin].
 */
internal
val `OrgGradlePluginGroup`.`assembler`: PluginDependencySpec
    get() = plugins.id("org.gradle.assembler")


/**
 * The `org.gradle.assembler-lang` plugin implemented by [org.gradle.language.assembler.plugins.AssemblerLangPlugin].
 */
internal
val `OrgGradlePluginGroup`.`assembler-lang`: PluginDependencySpec
    get() = plugins.id("org.gradle.assembler-lang")


/**
 * The `org.gradle.base` plugin implemented by [org.gradle.api.plugins.BasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`base`: PluginDependencySpec
    get() = plugins.id("org.gradle.base")


/**
 * The `org.gradle.binary-base` plugin implemented by [org.gradle.platform.base.plugins.BinaryBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`binary-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.binary-base")


/**
 * The `org.gradle.build-dashboard` plugin implemented by [org.gradle.api.reporting.plugins.BuildDashboardPlugin].
 */
internal
val `OrgGradlePluginGroup`.`build-dashboard`: PluginDependencySpec
    get() = plugins.id("org.gradle.build-dashboard")


/**
 * The `org.gradle.build-init` plugin implemented by [org.gradle.buildinit.plugins.BuildInitPlugin].
 */
internal
val `OrgGradlePluginGroup`.`build-init`: PluginDependencySpec
    get() = plugins.id("org.gradle.build-init")


/**
 * The `org.gradle.c` plugin implemented by [org.gradle.language.c.plugins.CPlugin].
 */
internal
val `OrgGradlePluginGroup`.`c`: PluginDependencySpec
    get() = plugins.id("org.gradle.c")


/**
 * The `org.gradle.c-lang` plugin implemented by [org.gradle.language.c.plugins.CLangPlugin].
 */
internal
val `OrgGradlePluginGroup`.`c-lang`: PluginDependencySpec
    get() = plugins.id("org.gradle.c-lang")


/**
 * The `org.gradle.checkstyle` plugin implemented by [org.gradle.api.plugins.quality.CheckstylePlugin].
 */
internal
val `OrgGradlePluginGroup`.`checkstyle`: PluginDependencySpec
    get() = plugins.id("org.gradle.checkstyle")


/**
 * The `org.gradle.clang-compiler` plugin implemented by [org.gradle.nativeplatform.toolchain.plugins.ClangCompilerPlugin].
 */
internal
val `OrgGradlePluginGroup`.`clang-compiler`: PluginDependencySpec
    get() = plugins.id("org.gradle.clang-compiler")


/**
 * The `org.gradle.codenarc` plugin implemented by [org.gradle.api.plugins.quality.CodeNarcPlugin].
 */
internal
val `OrgGradlePluginGroup`.`codenarc`: PluginDependencySpec
    get() = plugins.id("org.gradle.codenarc")


/**
 * The `org.gradle.component-base` plugin implemented by [org.gradle.platform.base.plugins.ComponentBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`component-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.component-base")


/**
 * The `org.gradle.component-model-base` plugin implemented by [org.gradle.language.base.plugins.ComponentModelBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`component-model-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.component-model-base")


/**
 * The `org.gradle.cpp` plugin implemented by [org.gradle.language.cpp.plugins.CppPlugin].
 */
internal
val `OrgGradlePluginGroup`.`cpp`: PluginDependencySpec
    get() = plugins.id("org.gradle.cpp")


/**
 * The `org.gradle.cpp-application` plugin implemented by [org.gradle.language.cpp.plugins.CppApplicationPlugin].
 */
internal
val `OrgGradlePluginGroup`.`cpp-application`: PluginDependencySpec
    get() = plugins.id("org.gradle.cpp-application")


/**
 * The `org.gradle.cpp-lang` plugin implemented by [org.gradle.language.cpp.plugins.CppLangPlugin].
 */
internal
val `OrgGradlePluginGroup`.`cpp-lang`: PluginDependencySpec
    get() = plugins.id("org.gradle.cpp-lang")


/**
 * The `org.gradle.cpp-library` plugin implemented by [org.gradle.language.cpp.plugins.CppLibraryPlugin].
 */
internal
val `OrgGradlePluginGroup`.`cpp-library`: PluginDependencySpec
    get() = plugins.id("org.gradle.cpp-library")


/**
 * The `org.gradle.cpp-unit-test` plugin implemented by [org.gradle.nativeplatform.test.cpp.plugins.CppUnitTestPlugin].
 */
internal
val `OrgGradlePluginGroup`.`cpp-unit-test`: PluginDependencySpec
    get() = plugins.id("org.gradle.cpp-unit-test")


/**
 * The `org.gradle.cunit` plugin implemented by [org.gradle.nativeplatform.test.cunit.plugins.CUnitConventionPlugin].
 */
internal
val `OrgGradlePluginGroup`.`cunit`: PluginDependencySpec
    get() = plugins.id("org.gradle.cunit")


/**
 * The `org.gradle.cunit-test-suite` plugin implemented by [org.gradle.nativeplatform.test.cunit.plugins.CUnitPlugin].
 */
internal
val `OrgGradlePluginGroup`.`cunit-test-suite`: PluginDependencySpec
    get() = plugins.id("org.gradle.cunit-test-suite")


/**
 * The `org.gradle.distribution` plugin implemented by [org.gradle.api.distribution.plugins.DistributionPlugin].
 */
internal
val `OrgGradlePluginGroup`.`distribution`: PluginDependencySpec
    get() = plugins.id("org.gradle.distribution")


/**
 * The `org.gradle.ear` plugin implemented by [org.gradle.plugins.ear.EarPlugin].
 */
internal
val `OrgGradlePluginGroup`.`ear`: PluginDependencySpec
    get() = plugins.id("org.gradle.ear")


/**
 * The `org.gradle.eclipse` plugin implemented by [org.gradle.plugins.ide.eclipse.EclipsePlugin].
 */
internal
val `OrgGradlePluginGroup`.`eclipse`: PluginDependencySpec
    get() = plugins.id("org.gradle.eclipse")


/**
 * The `org.gradle.eclipse-wtp` plugin implemented by [org.gradle.plugins.ide.eclipse.EclipseWtpPlugin].
 */
internal
val `OrgGradlePluginGroup`.`eclipse-wtp`: PluginDependencySpec
    get() = plugins.id("org.gradle.eclipse-wtp")


/**
 * The `org.gradle.gcc-compiler` plugin implemented by [org.gradle.nativeplatform.toolchain.plugins.GccCompilerPlugin].
 */
internal
val `OrgGradlePluginGroup`.`gcc-compiler`: PluginDependencySpec
    get() = plugins.id("org.gradle.gcc-compiler")


/**
 * The `org.gradle.google-test` plugin implemented by [org.gradle.nativeplatform.test.googletest.plugins.GoogleTestConventionPlugin].
 */
internal
val `OrgGradlePluginGroup`.`google-test`: PluginDependencySpec
    get() = plugins.id("org.gradle.google-test")


/**
 * The `org.gradle.google-test-test-suite` plugin implemented by [org.gradle.nativeplatform.test.googletest.plugins.GoogleTestPlugin].
 */
internal
val `OrgGradlePluginGroup`.`google-test-test-suite`: PluginDependencySpec
    get() = plugins.id("org.gradle.google-test-test-suite")


/**
 * The `org.gradle.groovy` plugin implemented by [org.gradle.api.plugins.GroovyPlugin].
 */
internal
val `OrgGradlePluginGroup`.`groovy`: PluginDependencySpec
    get() = plugins.id("org.gradle.groovy")


/**
 * The `org.gradle.groovy-base` plugin implemented by [org.gradle.api.plugins.GroovyBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`groovy-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.groovy-base")


/**
 * The `org.gradle.groovy-gradle-plugin` plugin implemented by [org.gradle.plugin.devel.internal.precompiled.PrecompiledGroovyPluginsPlugin].
 */
internal
val `OrgGradlePluginGroup`.`groovy-gradle-plugin`: PluginDependencySpec
    get() = plugins.id("org.gradle.groovy-gradle-plugin")


/**
 * The `org.gradle.help-tasks` plugin implemented by [org.gradle.api.plugins.HelpTasksPlugin].
 */
internal
val `OrgGradlePluginGroup`.`help-tasks`: PluginDependencySpec
    get() = plugins.id("org.gradle.help-tasks")


/**
 * The `org.gradle.idea` plugin implemented by [org.gradle.plugins.ide.idea.IdeaPlugin].
 */
internal
val `OrgGradlePluginGroup`.`idea`: PluginDependencySpec
    get() = plugins.id("org.gradle.idea")


/**
 * The `org.gradle.ivy-publish` plugin implemented by [org.gradle.api.publish.ivy.plugins.IvyPublishPlugin].
 */
internal
val `OrgGradlePluginGroup`.`ivy-publish`: PluginDependencySpec
    get() = plugins.id("org.gradle.ivy-publish")


/**
 * The `org.gradle.jacoco` plugin implemented by [org.gradle.testing.jacoco.plugins.JacocoPlugin].
 */
internal
val `OrgGradlePluginGroup`.`jacoco`: PluginDependencySpec
    get() = plugins.id("org.gradle.jacoco")


/**
 * The `org.gradle.jacoco-report-aggregation` plugin implemented by [org.gradle.testing.jacoco.plugins.JacocoReportAggregationPlugin].
 */
internal
val `OrgGradlePluginGroup`.`jacoco-report-aggregation`: PluginDependencySpec
    get() = plugins.id("org.gradle.jacoco-report-aggregation")


/**
 * The `org.gradle.java` plugin implemented by [org.gradle.api.plugins.JavaPlugin].
 */
internal
val `OrgGradlePluginGroup`.`java`: PluginDependencySpec
    get() = plugins.id("org.gradle.java")


/**
 * The `org.gradle.java-base` plugin implemented by [org.gradle.api.plugins.JavaBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`java-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.java-base")


/**
 * The `org.gradle.java-gradle-plugin` plugin implemented by [org.gradle.plugin.devel.plugins.JavaGradlePluginPlugin].
 */
internal
val `OrgGradlePluginGroup`.`java-gradle-plugin`: PluginDependencySpec
    get() = plugins.id("org.gradle.java-gradle-plugin")


/**
 * The `org.gradle.java-library` plugin implemented by [org.gradle.api.plugins.JavaLibraryPlugin].
 */
internal
val `OrgGradlePluginGroup`.`java-library`: PluginDependencySpec
    get() = plugins.id("org.gradle.java-library")


/**
 * The `org.gradle.java-library-distribution` plugin implemented by [org.gradle.api.plugins.JavaLibraryDistributionPlugin].
 */
internal
val `OrgGradlePluginGroup`.`java-library-distribution`: PluginDependencySpec
    get() = plugins.id("org.gradle.java-library-distribution")


/**
 * The `org.gradle.java-platform` plugin implemented by [org.gradle.api.plugins.JavaPlatformPlugin].
 */
internal
val `OrgGradlePluginGroup`.`java-platform`: PluginDependencySpec
    get() = plugins.id("org.gradle.java-platform")


/**
 * The `org.gradle.java-test-fixtures` plugin implemented by [org.gradle.api.plugins.JavaTestFixturesPlugin].
 */
internal
val `OrgGradlePluginGroup`.`java-test-fixtures`: PluginDependencySpec
    get() = plugins.id("org.gradle.java-test-fixtures")


/**
 * The `org.gradle.jvm-ecosystem` plugin implemented by [org.gradle.api.plugins.JvmEcosystemPlugin].
 */
internal
val `OrgGradlePluginGroup`.`jvm-ecosystem`: PluginDependencySpec
    get() = plugins.id("org.gradle.jvm-ecosystem")


/**
 * The `org.gradle.jvm-test-suite` plugin implemented by [org.gradle.api.plugins.JvmTestSuitePlugin].
 */
internal
val `OrgGradlePluginGroup`.`jvm-test-suite`: PluginDependencySpec
    get() = plugins.id("org.gradle.jvm-test-suite")


/**
 * The `org.gradle.jvm-toolchain-management` plugin implemented by [org.gradle.api.plugins.JvmToolchainManagementPlugin].
 */
internal
val `OrgGradlePluginGroup`.`jvm-toolchain-management`: PluginDependencySpec
    get() = plugins.id("org.gradle.jvm-toolchain-management")


/**
 * The `org.gradle.jvm-toolchains` plugin implemented by [org.gradle.api.plugins.JvmToolchainsPlugin].
 */
internal
val `OrgGradlePluginGroup`.`jvm-toolchains`: PluginDependencySpec
    get() = plugins.id("org.gradle.jvm-toolchains")


/**
 * The `org.gradle.language-base` plugin implemented by [org.gradle.language.base.plugins.LanguageBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`language-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.language-base")


/**
 * The `org.gradle.lifecycle-base` plugin implemented by [org.gradle.language.base.plugins.LifecycleBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`lifecycle-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.lifecycle-base")


/**
 * The `org.gradle.maven-publish` plugin implemented by [org.gradle.api.publish.maven.plugins.MavenPublishPlugin].
 */
internal
val `OrgGradlePluginGroup`.`maven-publish`: PluginDependencySpec
    get() = plugins.id("org.gradle.maven-publish")


/**
 * The `org.gradle.microsoft-visual-cpp-compiler` plugin implemented by [org.gradle.nativeplatform.toolchain.plugins.MicrosoftVisualCppCompilerPlugin].
 */
internal
val `OrgGradlePluginGroup`.`microsoft-visual-cpp-compiler`: PluginDependencySpec
    get() = plugins.id("org.gradle.microsoft-visual-cpp-compiler")


/**
 * The `org.gradle.native-component` plugin implemented by [org.gradle.nativeplatform.plugins.NativeComponentPlugin].
 */
internal
val `OrgGradlePluginGroup`.`native-component`: PluginDependencySpec
    get() = plugins.id("org.gradle.native-component")


/**
 * The `org.gradle.native-component-model` plugin implemented by [org.gradle.nativeplatform.plugins.NativeComponentModelPlugin].
 */
internal
val `OrgGradlePluginGroup`.`native-component-model`: PluginDependencySpec
    get() = plugins.id("org.gradle.native-component-model")


/**
 * The `org.gradle.objective-c` plugin implemented by [org.gradle.language.objectivec.plugins.ObjectiveCPlugin].
 */
internal
val `OrgGradlePluginGroup`.`objective-c`: PluginDependencySpec
    get() = plugins.id("org.gradle.objective-c")


/**
 * The `org.gradle.objective-c-lang` plugin implemented by [org.gradle.language.objectivec.plugins.ObjectiveCLangPlugin].
 */
internal
val `OrgGradlePluginGroup`.`objective-c-lang`: PluginDependencySpec
    get() = plugins.id("org.gradle.objective-c-lang")


/**
 * The `org.gradle.objective-cpp` plugin implemented by [org.gradle.language.objectivecpp.plugins.ObjectiveCppPlugin].
 */
internal
val `OrgGradlePluginGroup`.`objective-cpp`: PluginDependencySpec
    get() = plugins.id("org.gradle.objective-cpp")


/**
 * The `org.gradle.objective-cpp-lang` plugin implemented by [org.gradle.language.objectivecpp.plugins.ObjectiveCppLangPlugin].
 */
internal
val `OrgGradlePluginGroup`.`objective-cpp-lang`: PluginDependencySpec
    get() = plugins.id("org.gradle.objective-cpp-lang")


/**
 * The `org.gradle.pmd` plugin implemented by [org.gradle.api.plugins.quality.PmdPlugin].
 */
internal
val `OrgGradlePluginGroup`.`pmd`: PluginDependencySpec
    get() = plugins.id("org.gradle.pmd")


/**
 * The `org.gradle.project-report` plugin implemented by [org.gradle.api.plugins.ProjectReportsPlugin].
 */
internal
val `OrgGradlePluginGroup`.`project-report`: PluginDependencySpec
    get() = plugins.id("org.gradle.project-report")


/**
 * The `org.gradle.project-reports` plugin implemented by [org.gradle.api.plugins.ProjectReportsPlugin].
 */
internal
val `OrgGradlePluginGroup`.`project-reports`: PluginDependencySpec
    get() = plugins.id("org.gradle.project-reports")


/**
 * The `org.gradle.publishing` plugin implemented by [org.gradle.api.publish.plugins.PublishingPlugin].
 */
internal
val `OrgGradlePluginGroup`.`publishing`: PluginDependencySpec
    get() = plugins.id("org.gradle.publishing")


/**
 * The `org.gradle.reporting-base` plugin implemented by [org.gradle.api.plugins.ReportingBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`reporting-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.reporting-base")


/**
 * The `org.gradle.scala` plugin implemented by [org.gradle.api.plugins.scala.ScalaPlugin].
 */
internal
val `OrgGradlePluginGroup`.`scala`: PluginDependencySpec
    get() = plugins.id("org.gradle.scala")


/**
 * The `org.gradle.scala-base` plugin implemented by [org.gradle.api.plugins.scala.ScalaBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`scala-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.scala-base")


/**
 * The `org.gradle.signing` plugin implemented by [org.gradle.plugins.signing.SigningPlugin].
 */
internal
val `OrgGradlePluginGroup`.`signing`: PluginDependencySpec
    get() = plugins.id("org.gradle.signing")


/**
 * The `org.gradle.standard-tool-chains` plugin implemented by [org.gradle.nativeplatform.toolchain.internal.plugins.StandardToolChainsPlugin].
 */
internal
val `OrgGradlePluginGroup`.`standard-tool-chains`: PluginDependencySpec
    get() = plugins.id("org.gradle.standard-tool-chains")


/**
 * The `org.gradle.swift-application` plugin implemented by [org.gradle.language.swift.plugins.SwiftApplicationPlugin].
 */
internal
val `OrgGradlePluginGroup`.`swift-application`: PluginDependencySpec
    get() = plugins.id("org.gradle.swift-application")


/**
 * The `org.gradle.swift-library` plugin implemented by [org.gradle.language.swift.plugins.SwiftLibraryPlugin].
 */
internal
val `OrgGradlePluginGroup`.`swift-library`: PluginDependencySpec
    get() = plugins.id("org.gradle.swift-library")


/**
 * The `org.gradle.swiftpm-export` plugin implemented by [org.gradle.swiftpm.plugins.SwiftPackageManagerExportPlugin].
 */
internal
val `OrgGradlePluginGroup`.`swiftpm-export`: PluginDependencySpec
    get() = plugins.id("org.gradle.swiftpm-export")


/**
 * The `org.gradle.test-report-aggregation` plugin implemented by [org.gradle.api.plugins.TestReportAggregationPlugin].
 */
internal
val `OrgGradlePluginGroup`.`test-report-aggregation`: PluginDependencySpec
    get() = plugins.id("org.gradle.test-report-aggregation")


/**
 * The `org.gradle.test-suite-base` plugin implemented by [org.gradle.testing.base.plugins.TestSuiteBasePlugin].
 */
internal
val `OrgGradlePluginGroup`.`test-suite-base`: PluginDependencySpec
    get() = plugins.id("org.gradle.test-suite-base")


/**
 * The `org.gradle.version-catalog` plugin implemented by [org.gradle.api.plugins.catalog.VersionCatalogPlugin].
 */
internal
val `OrgGradlePluginGroup`.`version-catalog`: PluginDependencySpec
    get() = plugins.id("org.gradle.version-catalog")


/**
 * The `org.gradle.visual-studio` plugin implemented by [org.gradle.ide.visualstudio.plugins.VisualStudioPlugin].
 */
internal
val `OrgGradlePluginGroup`.`visual-studio`: PluginDependencySpec
    get() = plugins.id("org.gradle.visual-studio")


/**
 * The `org.gradle.war` plugin implemented by [org.gradle.api.plugins.WarPlugin].
 */
internal
val `OrgGradlePluginGroup`.`war`: PluginDependencySpec
    get() = plugins.id("org.gradle.war")


/**
 * The `org.gradle.windows-resource-script` plugin implemented by [org.gradle.language.rc.plugins.WindowsResourceScriptPlugin].
 */
internal
val `OrgGradlePluginGroup`.`windows-resource-script`: PluginDependencySpec
    get() = plugins.id("org.gradle.windows-resource-script")


/**
 * The `org.gradle.windows-resources` plugin implemented by [org.gradle.language.rc.plugins.WindowsResourcesPlugin].
 */
internal
val `OrgGradlePluginGroup`.`windows-resources`: PluginDependencySpec
    get() = plugins.id("org.gradle.windows-resources")


/**
 * The `org.gradle.wrapper` plugin implemented by [org.gradle.buildinit.plugins.WrapperPlugin].
 */
internal
val `OrgGradlePluginGroup`.`wrapper`: PluginDependencySpec
    get() = plugins.id("org.gradle.wrapper")


/**
 * The `org.gradle.xcode` plugin implemented by [org.gradle.ide.xcode.plugins.XcodePlugin].
 */
internal
val `OrgGradlePluginGroup`.`xcode`: PluginDependencySpec
    get() = plugins.id("org.gradle.xcode")


/**
 * The `org.gradle.xctest` plugin implemented by [org.gradle.nativeplatform.test.xctest.plugins.XCTestConventionPlugin].
 */
internal
val `OrgGradlePluginGroup`.`xctest`: PluginDependencySpec
    get() = plugins.id("org.gradle.xctest")


/**
 * The `org.jetbrains` plugin group.
 */
@org.gradle.api.Generated
internal
class `OrgJetbrainsPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `org.jetbrains`.
 */
internal
val `OrgPluginGroup`.`jetbrains`: `OrgJetbrainsPluginGroup`
    get() = `OrgJetbrainsPluginGroup`(plugins)


/**
 * The `org.jetbrains.gradle` plugin group.
 */
@org.gradle.api.Generated
internal
class `OrgJetbrainsGradlePluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `org.jetbrains.gradle`.
 */
internal
val `OrgJetbrainsPluginGroup`.`gradle`: `OrgJetbrainsGradlePluginGroup`
    get() = `OrgJetbrainsGradlePluginGroup`(plugins)


/**
 * The `org.jetbrains.gradle.plugin` plugin group.
 */
@org.gradle.api.Generated
internal
class `OrgJetbrainsGradlePluginPluginGroup`(internal val plugins: PluginDependenciesSpec)


/**
 * Plugin ids starting with `org.jetbrains.gradle.plugin`.
 */
internal
val `OrgJetbrainsGradlePluginGroup`.`plugin`: `OrgJetbrainsGradlePluginPluginGroup`
    get() = `OrgJetbrainsGradlePluginPluginGroup`(plugins)


/**
 * The `org.jetbrains.gradle.plugin.idea-ext` plugin implemented by [org.jetbrains.gradle.ext.IdeaExtPlugin].
 */
internal
val `OrgJetbrainsGradlePluginPluginGroup`.`idea-ext`: PluginDependencySpec
    get() = plugins.id("org.jetbrains.gradle.plugin.idea-ext")

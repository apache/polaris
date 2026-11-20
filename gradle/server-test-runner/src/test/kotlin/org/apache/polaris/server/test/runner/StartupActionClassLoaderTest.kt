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
package org.apache.polaris.server.test.runner

import java.nio.file.Path
import javax.tools.ToolProvider
import kotlin.io.path.createDirectories
import kotlin.io.path.writeText
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupAction
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupContext
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class StartupActionClassLoaderTest {
  @TempDir lateinit var directory: Path

  @Test
  fun `loads action dependency without using the plugin version`() {
    val source = directory.resolve("ParentDependency.java")
    source.writeText(
      """
      package org.apache.polaris.server.test.runner;
      public class ParentDependency {
        public static String version() { return "action"; }
      }
      """
        .trimIndent(),
    )
    val classes = directory.resolve("classes").createDirectories()
    assertThat(
        ToolProvider.getSystemJavaCompiler()
          .run(
            null,
            null,
            null,
            "-d",
            classes.toString(),
            source.toString(),
          ),
      )
      .isZero()
    assertThat(ParentDependency.version()).isEqualTo("plugin")

    StartupActionClassLoader(arrayOf(classes.toUri().toURL())).use { loader ->
      val dependency = loader.loadClass(ParentDependency::class.java.name)
      assertThat(dependency.getMethod("version").invoke(null)).isEqualTo("action")
      assertThat(dependency).isNotSameAs(ParentDependency::class.java)
    }
  }

  @Test
  fun `shares SPI identity even when SPI classes are on the action classpath`() {
    val spi = PolarisServerStartupAction::class.java
    StartupActionClassLoader(arrayOf(spi.protectionDomain.codeSource.location)).use { loader ->
      assertThat(loader.loadClass(spi.name)).isSameAs(spi)
      assertThat(loader.loadClass(PolarisServerStartupContext::class.java.name))
        .isSameAs(PolarisServerStartupContext::class.java)
      assertThat(loader.loadClass("java.sql.Driver")).isSameAs(java.sql.Driver::class.java)
    }
  }

  @Test
  fun `does not expose plugin classes or resources absent from action classpath`() {
    val type = ParentDependency::class.java
    val resource = type.name.replace('.', '/') + ".class"
    assertThat(type.classLoader.getResource(resource)).isNotNull()
    StartupActionClassLoader(emptyArray()).use { loader ->
      assertThatThrownBy { loader.loadClass(type.name) }
        .isInstanceOf(ClassNotFoundException::class.java)
      assertThat(loader.getResource(resource)).isNull()
      assertThat(loader.getResources(resource).toList()).isEmpty()
    }
  }
}

class ParentDependency {
  companion object {
    @JvmStatic fun version(): String = "plugin"
  }
}

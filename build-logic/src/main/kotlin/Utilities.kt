/*
 * Copyright (C) 2024 Dremio
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

import java.io.File
import java.io.FileOutputStream
import java.nio.file.attribute.FileTime
import java.util.zip.ZipFile
import java.util.zip.ZipOutputStream
import org.gradle.api.Project
import org.gradle.process.JavaForkOptions

/**
 * Extract the scala version from polaris spark project, and points the build directory to a sub-dir
 * that uses scala version as name. The polaris spark project name is in format of
 * <project>-<sparkVersion>_<scalaVersion>, for example: polaris-spark-3.5_2.12.
 */
fun Project.getAndUseScalaVersionForProject(): String {
  val sparkScala = project.name.split("-").last().split("_")

  val scalaVersion = sparkScala[1]

  // direct the build to build/<scalaVersion> to avoid potential collision problem
  project.layout.buildDirectory.set(layout.buildDirectory.dir(scalaVersion).get())

  return scalaVersion
}

/**
 * Adds the JPMS options required for Spark to run on Java 17, taken from the
 * `DEFAULT_MODULE_OPTIONS` constant in `org.apache.spark.launcher.JavaModuleOptions`.
 */
fun JavaForkOptions.addSparkJvmOptions() {
  jvmArgs =
    (jvmArgs ?: emptyList()) +
      listOf(
        // Spark 3.3+
        "-XX:+IgnoreUnrecognizedVMOptions",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
        // Spark 3.4+
        "-Djdk.reflect.useDirectMethodHandle=false",
      )
}

/**
 * Rewrites the given ZIP file.
 *
 * The timestamps of all entries are set to `1980-02-01 00:00`, zip entries appear in a
 * deterministic order.
 */
fun makeZipReproducible(source: File) {
  val t = FileTime.fromMillis(318211200_000) // 1980-02-01 00:00 GMT

  val outFile = File(source.absolutePath + ".tmp.out")

  val names = mutableListOf<String>()
  ZipFile(source).use { zip -> zip.stream().forEach { e -> names.add(e.name) } }
  names.sort()

  ZipOutputStream(FileOutputStream(outFile)).use { dst ->
    ZipFile(source).use { zip ->
      names.forEach { n ->
        val e = zip.getEntry(n)
        zip.getInputStream(e).use { src ->
          e.setCreationTime(t)
          e.setLastAccessTime(t)
          e.setLastModifiedTime(t)
          dst.putNextEntry(e)
          src.copyTo(dst)
          dst.closeEntry()
          src.close()
        }
      }
    }
  }

  val origFile = File(source.absolutePath + ".tmp.orig")
  source.renameTo(origFile)
  outFile.renameTo(source)
  origFile.delete()
}

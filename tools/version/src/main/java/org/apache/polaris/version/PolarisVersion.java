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
package org.apache.polaris.version;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Utility class to retrieve the current Polaris version and the contents of the {@code NOTICE} and
 * {@code LICENSE*} files.
 *
 * <p>If built as a release using the Gradle {@code -Prelease} project property, more information
 * like the Git tag, Git commit ID, build system and more is available as well.
 */
public final class PolarisVersion {
  private PolarisVersion() {}

  /** The version string as in the file {@code version.txt} in the project's root directory. */
  public static String polarisVersionString() {
    return PolarisVersionNumber.POLARIS_VERSION;
  }

  /**
   * Flag whether the build is a released version, when Polaris has been built with the Gradle
   * {@code -Prelease} project property. If {@code true}, the {@code getBuild*} functions return
   * meaningful values.
   */
  public static boolean isReleaseBuild() {
    return "true".equals(PolarisVersionJarInfo.buildInfo(MF_IS_RELEASE).orElse("false"));
  }

  /**
   * Returns the version as in the jar manifest, if {@linkplain #isReleaseBuild() build-time Git
   * information} is available, when Polaris has been built with the Gradle {@code -Prelease}
   * project property..
   *
   * <p>Example values: {@code 1.0.0-incubating-SNAPSHOT}, {@code 1.0.0-incubating}, {@code 1.0.0}
   *
   * @see #isReleaseBuild()
   */
  public static Optional<String> getBuildReleasedVersion() {
    return PolarisVersionJarInfo.buildInfo(MF_VERSION);
  }

  /**
   * Returns the commit ID as in the jar manifest, if {@linkplain #isReleaseBuild() build-time Git
   * information} is available, when Polaris has been built with the Gradle {@code -Prelease}
   * project property.
   *
   * <p>Example value: {@code d417725ec7c88c1ee8f940ffb2ce72aec7fb2a17}
   *
   * @see #isReleaseBuild()
   */
  public static Optional<String> getBuildGitHead() {
    return PolarisVersionJarInfo.buildInfo(MF_BUILD_GIT_HEAD);
  }

  /**
   * Returns the output of {@code git describe --tags} as in the jar manifest, if {@linkplain
   * #isReleaseBuild() build-time Git information} is available, when Polaris has been built with
   * the Gradle {@code -Prelease} project property.
   *
   * <p>Example value: {@code apache-polaris-0.1.2}
   *
   * @see #isReleaseBuild()
   */
  public static Optional<String> getBuildGitTag() {
    return PolarisVersionJarInfo.buildInfo(MF_BUILD_GIT_DESCRIBE);
  }

  /**
   * Returns the Java <em>specification</em> version used during the build as in the jar manifest,
   * if {@linkplain #isReleaseBuild() build-time Git information} is available, when Polaris has
   * been built with the Gradle {@code -Prelease} project property.
   *
   * <p>Example value: {@code 21}
   *
   * @see #isReleaseBuild()
   */
  public static Optional<String> getBuildJavaSpecificationVersion() {
    return PolarisVersionJarInfo.buildInfo(MF_BUILD_JAVA_SPECIFICATION_VERSION);
  }

  public static String readNoticeFile() {
    return readResource("NOTICE");
  }

  public static String readSourceLicenseFile() {
    return readResource("LICENSE");
  }

  /*
  public static String readBinaryLicenseFile() {
    return readResource("LICENSE-BINARY-DIST");
  }
   */

  private static final String MF_VERSION = "Apache-Polaris-Version";
  private static final String MF_IS_RELEASE = "Apache-Polaris-Is-Release";
  private static final String MF_BUILD_GIT_HEAD = "Apache-Polaris-Build-Git-Head";
  private static final String MF_BUILD_GIT_DESCRIBE = "Apache-Polaris-Build-Git-Describe";
  private static final String MF_BUILD_JAVA_SPECIFICATION_VERSION =
      "Apache-Polaris-Build-Java-Specification-Version";
  private static final List<String> MF_ALL =
      List.of(
          MF_VERSION,
          MF_IS_RELEASE,
          MF_BUILD_GIT_HEAD,
          MF_BUILD_GIT_DESCRIBE,
          MF_BUILD_JAVA_SPECIFICATION_VERSION);

  static String readResource(String resource) {
    var fullResource = format("/META-INF/resources/apache-polaris/%s.txt", resource);
    var resourceUrl =
        requireNonNull(
            PolarisVersion.class.getResource(fullResource),
            "Resource " + fullResource + " does not exist");
    try (InputStream in = resourceUrl.openConnection().getInputStream()) {
      return new String(in.readAllBytes(), UTF_8);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load resource " + fullResource, e);
    }
  }

  // Couple inner classes to leverage Java's class loading mechanism as singletons and for
  // initialization. Package-protected for testing.

  static final class PolarisVersionJarInfo {
    static final Map<String, String> BUILD_INFO = new HashMap<>();

    static Optional<String> buildInfo(String key) {
      return Optional.ofNullable(BUILD_INFO.get(key));
    }

    static {
      loadManifest("MANIFEST.MF");
    }

    static void loadManifest(String manifestFile) {
      var polarisVersionResource = PolarisVersionResource.POLARIS_VERSION_RESOURCE;
      if ("jar".equals(polarisVersionResource.getProtocol())) {
        var path = polarisVersionResource.toString();
        var jarSep = path.lastIndexOf('!');
        if (jarSep == -1) {
          throw new IllegalStateException(
              "Could not determine the jar of the Apache Polaris version artifact: " + path);
        }
        var manifestPath = path.substring(0, jarSep + 1) + "/META-INF/" + manifestFile;
        try {
          try (InputStream in =
              URI.create(manifestPath).toURL().openConnection().getInputStream()) {
            var manifest = new Manifest(in);
            var attributes = manifest.getMainAttributes();
            MF_ALL.stream()
                .map(Attributes.Name::new)
                .filter(attributes::containsKey)
                .forEach(k -> BUILD_INFO.put(k.toString(), attributes.getValue(k)));
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  static final class PolarisVersionNumber {
    static final String POLARIS_VERSION;

    static {
      try (InputStream in =
          PolarisVersionResource.POLARIS_VERSION_RESOURCE.openConnection().getInputStream()) {
        Properties p = new Properties();
        p.load(in);
        POLARIS_VERSION = p.getProperty("polaris.version");
      } catch (IOException e) {
        throw new RuntimeException(
            "Failed to load Apache Polaris version from org/apache/polaris/version/polaris-version.properties resource",
            e);
      }
    }
  }

  static final class PolarisVersionResource {
    static final URL POLARIS_VERSION_RESOURCE =
        requireNonNull(
            PolarisVersion.class.getResource("polaris-version.properties"),
            "Resource org/apache/polaris/version/polaris-version.properties containing the Apache Polaris version does not exist");
  }
}

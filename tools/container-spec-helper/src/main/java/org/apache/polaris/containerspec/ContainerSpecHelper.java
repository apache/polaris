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
package org.apache.polaris.containerspec;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Locale;
import java.util.Objects;
import org.testcontainers.utility.DockerImageName;

// Spotless/Java doesn't really "like" Javadoc @snippet
// spotless:off
/**
 * Helper functionality when using testcontainers to resolve the "full" docker image name (name and
 * tag) via a {@code Dockerfile}, which is kept up to date using Renovate.
 *
 * <p>Requires a file {@code Dockerfile-<NAME>-version} as a resource on the classpath in the
 * package of the given {@code containerClass}. {@code <NAME>} is replaced with the {@code name}
 * parameter passed to {@link #containerSpecHelper(String, Class)}.
 *
 * <h2>Example
 *
 * <p>Given an example test code like the following.
 *
 * {@snippet :
 * package my.code;
 *
 * import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;
 * import org.testcontainers.utility.DockerImageName;
 *
 * class MyTestCode {
 *
 *   void codeThatNeedsTheContainer() {
 *     DockerImageName dockerImageName = containerSpecHelper("mongodb", MyTestCode.class)
 *       .dockerImageName(null)
 *       .asCompatibleSubstituteFor("mongo");
 *   }
 *
 * }
 * }
 *
 * <p>The above requires the file {@code my/code/Dockerfile-mongodb-version} to be on the classpath,
 * containing just the Docker {@code FROM}, as in a file {@code
 * src/test/resources/my/code/Dockerfile-mongodb-version}.
 *
 * {@snippet :
 * FROM docker.io/mongo:8.0.4
 * }
 *
 * <p>Such resource files following the {@code Dockerfile} syntax can then be version-managed with
 * Renovate.
 */
// spotless:on
public final class ContainerSpecHelper {
  private final String name;
  private final Class<?> containerClass;

  public static ContainerSpecHelper containerSpecHelper(String name, Class<?> containerClass) {
    return new ContainerSpecHelper(name, containerClass);
  }

  private ContainerSpecHelper(String name, Class<?> containerClass) {
    this.name = name;
    this.containerClass = containerClass;
  }

  public String name() {
    return name;
  }

  public Class<?> containerClass() {
    return containerClass;
  }

  public DockerImageName dockerImageName(String explicitImageName) {
    if (explicitImageName != null) {
      return DockerImageName.parse(explicitImageName);
    }

    String dockerfile = format("Dockerfile-%s-version", name());
    URL resource = containerClass().getResource(dockerfile);
    Objects.requireNonNull(resource, dockerfile + " not found");

    String systemPropPrefix1 = "it.polaris.container." + name() + ".";
    String systemPropPrefix2 = "polaris.testing." + name() + ".";
    String envPrefix = name().toUpperCase(Locale.ROOT).replaceAll("-", "_") + "_DOCKER_";

    String explicitImage = System.getProperty(systemPropPrefix1 + "image");
    if (explicitImage == null) {
      explicitImage = System.getProperty(systemPropPrefix2 + "image");
    }
    if (explicitImage == null) {
      explicitImage = System.getenv(envPrefix + "IMAGE");
    }
    String explicitTag = System.getProperty(systemPropPrefix1 + "tag");
    if (explicitTag == null) {
      explicitTag = System.getProperty(systemPropPrefix2 + "tag");
    }
    if (explicitTag == null) {
      explicitTag = System.getenv(envPrefix + "TAG");
    }

    if (explicitImage != null && explicitTag != null) {
      return DockerImageName.parse(explicitImage + ':' + explicitTag);
    }

    try (InputStream in = resource.openConnection().getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, UTF_8))) {
      String fullImageName = null;
      String ln;
      while ((ln = reader.readLine()) != null) {
        ln = ln.trim();
        if (ln.startsWith("FROM ")) {
          fullImageName = ln.substring(5).trim();
          break;
        }
      }

      if (fullImageName == null) {
        throw new IllegalStateException(
            "Dockerfile " + dockerfile + " does not contain a line starting with 'FROM '");
      }

      if (explicitImage != null || explicitTag != null) {
        throw new IllegalArgumentException(
            "Must specify either BOTH, image name AND tag via system properties or environment  or omit and leave it to the default "
                + fullImageName
                + " from "
                + dockerfile);
      }

      return DockerImageName.parse(fullImageName);
    } catch (Exception e) {
      throw new RuntimeException("Failed to extract tag from " + resource, e);
    }
  }
}

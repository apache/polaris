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
package org.apache.polaris.containerspec;

import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.utility.DockerImageName;

class ContainerSpecHelperTest {

  private static final String CONTAINER_NAME = "test";
  private static final String DEFAULT_IMAGE = "docker.io/test/image:1.2.3";
  private static final String EXPECTED_PARTIAL_CONFIG_MESSAGE =
      "Must specify both image name and tag via system properties or environment variables, or omit both to use the default "
          + DEFAULT_IMAGE
          + " from Dockerfile-test-version";

  private static ContainerSpecHelper testHelper(
      Map<String, String> systemProperties, Map<String, String> environmentVariables) {
    return containerSpecHelper(
        CONTAINER_NAME,
        ContainerSpecHelperTest.class,
        systemProperties::get,
        environmentVariables::get);
  }

  @Test
  void dockerImageNameUsesDefaultFromDockerfileWhenNoOverrides() {
    DockerImageName imageName = testHelper(Map.of(), Map.of()).dockerImageName(null);

    assertThat(imageName).isEqualTo(DockerImageName.parse(DEFAULT_IMAGE));
  }

  @Test
  void dockerImageNameUsesExplicitImageNameWhenProvided() {
    DockerImageName imageName =
        testHelper(Map.of(), Map.of()).dockerImageName("docker.io/explicit:4.5.6");

    assertThat(imageName).isEqualTo(DockerImageName.parse("docker.io/explicit:4.5.6"));
  }

  @Test
  void dockerImageNameUsesBothSystemPropertiesWhenProvided() {
    DockerImageName imageName =
        testHelper(
                Map.of(
                    "polaris.testing.test.image",
                    "docker.io/custom/image",
                    "polaris.testing.test.tag",
                    "9.9.9"),
                Map.of())
            .dockerImageName(null);

    assertThat(imageName).isEqualTo(DockerImageName.parse("docker.io/custom/image:9.9.9"));
  }

  @Test
  void dockerImageNameUsesBothEnvironmentVariablesWhenProvided() {
    DockerImageName imageName =
        testHelper(
                Map.of(),
                Map.of("TEST_DOCKER_IMAGE", "docker.io/env/image", "TEST_DOCKER_TAG", "8.8.8"))
            .dockerImageName(null);

    assertThat(imageName).isEqualTo(DockerImageName.parse("docker.io/env/image:8.8.8"));
  }

  @Test
  void dockerImageNamePrefersSystemPropertyOverEnvironmentVariable() {
    DockerImageName imageName =
        testHelper(
                Map.of(
                    "polaris.testing.test.image",
                    "docker.io/prop/image",
                    "polaris.testing.test.tag",
                    "1.0.0"),
                Map.of("TEST_DOCKER_IMAGE", "docker.io/env/image", "TEST_DOCKER_TAG", "2.0.0"))
            .dockerImageName(null);

    assertThat(imageName).isEqualTo(DockerImageName.parse("docker.io/prop/image:1.0.0"));
  }

  @Test
  void dockerImageNameRejectsImageWithoutTag() {
    assertThatThrownBy(
            () ->
                testHelper(Map.of("polaris.testing.test.image", "docker.io/custom/image"), Map.of())
                    .dockerImageName(null))
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage(EXPECTED_PARTIAL_CONFIG_MESSAGE);
  }

  @Test
  void dockerImageNameRejectsTagWithoutImage() {
    assertThatThrownBy(
            () ->
                testHelper(Map.of("polaris.testing.test.tag", "9.9.9"), Map.of())
                    .dockerImageName(null))
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage(EXPECTED_PARTIAL_CONFIG_MESSAGE);
  }

  @Test
  void dockerImageNameRejectsImageWithoutTagFromEnvironmentVariable() {
    assertThatThrownBy(
            () ->
                testHelper(Map.of(), Map.of("TEST_DOCKER_IMAGE", "docker.io/env/image"))
                    .dockerImageName(null))
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage(EXPECTED_PARTIAL_CONFIG_MESSAGE);
  }

  @Test
  void dockerImageNameRejectsTagWithoutImageFromEnvironmentVariable() {
    assertThatThrownBy(
            () -> testHelper(Map.of(), Map.of("TEST_DOCKER_TAG", "8.8.8")).dockerImageName(null))
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage(EXPECTED_PARTIAL_CONFIG_MESSAGE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"it.polaris.container.test.image", "polaris.testing.test.image"})
  void dockerImageNameRejectsPartialImageOverrideFromAnySystemPropertyPrefix(String imageProperty) {
    assertThatThrownBy(
            () ->
                testHelper(Map.of(imageProperty, "docker.io/custom/image"), Map.of())
                    .dockerImageName(null))
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage(EXPECTED_PARTIAL_CONFIG_MESSAGE);
  }
}

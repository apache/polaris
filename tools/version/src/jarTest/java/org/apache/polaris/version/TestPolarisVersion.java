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
import static org.apache.polaris.version.PolarisVersion.getBuildGitHead;
import static org.apache.polaris.version.PolarisVersion.getBuildGitTag;
import static org.apache.polaris.version.PolarisVersion.getBuildJavaVersion;
import static org.apache.polaris.version.PolarisVersion.getBuildReleasedVersion;
import static org.apache.polaris.version.PolarisVersion.getBuildSystem;
import static org.apache.polaris.version.PolarisVersion.getBuildTimestamp;
import static org.apache.polaris.version.PolarisVersion.isReleaseBuild;
import static org.apache.polaris.version.PolarisVersion.polarisVersionString;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestPolarisVersion {
  @InjectSoftAssertions private SoftAssertions soft;

  /**
   * Test runs using a "non release" build, so the MANIFEST.MF file has no release version
   * information.
   */
  @Test
  @Order(1)
  public void versionAvailable() {
    soft.assertThat(polarisVersionString()).isEqualTo(System.getProperty("polarisVersion"));
    if (isReleaseBuild()) {
      soft.assertThat(getBuildReleasedVersion()).isNotEmpty();
      soft.assertThat(getBuildTimestamp()).isNotEmpty();
      soft.assertThat(getBuildGitHead()).isNotEmpty();
      soft.assertThat(getBuildGitTag()).isNotEmpty();
      soft.assertThat(getBuildSystem()).isNotEmpty();
      soft.assertThat(getBuildJavaVersion()).isNotEmpty();
    } else {
      soft.assertThat(getBuildReleasedVersion()).isEmpty();
      soft.assertThat(getBuildTimestamp()).isEmpty();
      soft.assertThat(getBuildGitHead()).isEmpty();
      soft.assertThat(getBuildGitTag()).isEmpty();
      soft.assertThat(getBuildSystem()).isEmpty();
      soft.assertThat(getBuildJavaVersion()).isEmpty();
    }
  }

  /**
   * Test runs using a static "release" build manifest, {@link
   * org.apache.polaris.version.PolarisVersion.PolarisVersionJarInfo#loadManifest(String)}
   * overwrites the already loaded build-info, so this test has to run <em>after</em> {@link
   * #versionAvailable()}.
   */
  @Test
  @Order(2)
  public void fakeReleaseManifest() {
    PolarisVersion.PolarisVersionJarInfo.loadManifest("FAKE_MANIFEST.MF");

    soft.assertThat(polarisVersionString()).isEqualTo(System.getProperty("polarisVersion"));
    soft.assertThat(isReleaseBuild()).isTrue();
    soft.assertThat(getBuildReleasedVersion()).contains("0.1.2-incubating-SNAPSHOT");
    soft.assertThat(getBuildTimestamp()).contains("2024-12-26-10:31:19+01:00");
    soft.assertThat(getBuildGitHead()).contains("27cf81929cbb08e545c8fcb1ed27a53d7ef1af79");
    soft.assertThat(getBuildGitTag()).contains("foo-tag-bar");
    soft.assertThat(getBuildSystem())
        .contains(
            "Linux myawesomehost 6.12.6 #81 SMP PREEMPT_DYNAMIC Fri Dec 20 09:22:38 CET 2024 x86_64 x86_64 x86_64 GNU/Linux");
    soft.assertThat(getBuildJavaVersion()).contains("21.0.5");
  }

  @Test
  public void versionTxtResource() {
    soft.assertThat(PolarisVersion.readResource("version").trim())
        .isEqualTo(System.getProperty("polarisVersion"));
  }

  @ParameterizedTest
  @MethodSource
  public void noticeLicense(String name, Supplier<String> supplier) throws Exception {
    var supplied = supplier.get();
    var expected =
        Files.readString(Paths.get(format("%s/%s", System.getProperty("rootProjectDir"), name)));
    soft.assertThat(supplied).isEqualTo(expected);
  }

  static Stream<Arguments> noticeLicense() {
    return Stream.of(
        Arguments.arguments("NOTICE", (Supplier<String>) PolarisVersion::readNoticeFile),
        Arguments.arguments("LICENSE", (Supplier<String>) PolarisVersion::readSourceLicenseFile));
    // Arguments.arguments(//   "LICENSE-BINARY-DIST", (Supplier<String>)
    // PolarisVersion::readBinaryLicenseFile));
  }
}

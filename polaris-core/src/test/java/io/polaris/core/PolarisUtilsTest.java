/*
 * Copyright (c) 2024 Snowflake Computing Inc.
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
package io.polaris.core;

import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit testing for `PolarisUtils` */
public class PolarisUtilsTest {

  @Test
  void testPathToDirectories() {
    Assertions.assertThat(PolarisUtils.pathToDirectories("s3://a/b/c/", 100))
        .contains(List.of("s3://", "s3://a/", "s3://a/b/", "s3://a/b/c/"));

    Assertions.assertThat(PolarisUtils.pathToDirectories("s3://a/b/c/", 1)).isEmpty();

    Assertions.assertThat(PolarisUtils.pathToDirectories("s3://a/b/c/", 4))
        .contains(List.of("s3://", "s3://a/", "s3://a/b/", "s3://a/b/c/"));

    Assertions.assertThat(PolarisUtils.pathToDirectories("s3://a/b/c/", 3)).isEmpty();
  }
}

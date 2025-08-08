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

package org.apache.polaris.service.catalog.common;

import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class LocationUtilsTest {

  @Test
  public void testHashFormat() {
    for (String input : List.of("", " ", "foo", "かな")) {
      String hash = LocationUtils.computeHash(input);
      Assertions.assertThat(hash).isNotNull();

      String[] parts = hash.split("/");
      Assertions.assertThat(parts).as("Hash must have exactly 4 segments").hasSize(4);

      Assertions.assertThat(parts[0])
          .as("First segment must be 4 chars")
          .hasSize(4)
          .matches("[01]+");

      Assertions.assertThat(parts[1])
          .as("Second segment must be 4 chars")
          .hasSize(4)
          .matches("[01]+");

      Assertions.assertThat(parts[2])
          .as("Third segment must be 4 chars")
          .hasSize(4)
          .matches("[01]+");

      Assertions.assertThat(parts[3])
          .as("Fourth segment must be 8 chars")
          .hasSize(8)
          .matches("[01]+");
    }
  }

  @Test
  public void testStableHashes() {
    Assertions.assertThat(LocationUtils.computeHash("foo")).isEqualTo("0101/1100/0100/00100000");
    Assertions.assertThat(LocationUtils.computeHash("foo")).isEqualTo("0101/1100/0100/00100000");
    Assertions.assertThat(LocationUtils.computeHash("foo "))
        .isNotEqualTo("0101/1100/0100/00100000");
    Assertions.assertThat(LocationUtils.computeHash(" foo"))
        .isNotEqualTo("0101/1100/0100/00100000");

    Assertions.assertThat(LocationUtils.computeHash("/some/path.txt"))
        .isEqualTo("1101/0101/1110/10001001");
    Assertions.assertThat(LocationUtils.computeHash("/other/path.txt"))
        .isEqualTo("1010/0010/1101/11011100");
    Assertions.assertThat(LocationUtils.computeHash("/some/path.txt/"))
        .isEqualTo("1110/1011/1111/11111010");
  }
}

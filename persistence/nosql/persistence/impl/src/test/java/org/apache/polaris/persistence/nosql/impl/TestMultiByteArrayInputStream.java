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
package org.apache.polaris.persistence.nosql.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestMultiByteArrayInputStream {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void reads(List<byte[]> bytes, String expected) throws IOException {
    soft.assertThat(new MultiByteArrayInputStream(bytes)).hasContent(expected);

    try (var in = new MultiByteArrayInputStream(bytes)) {
      soft.assertThat(new String(in.readAllBytes(), UTF_8)).isEqualTo(expected);
    }

    try (var in = new MultiByteArrayInputStream(bytes)) {
      var out = new ByteArrayOutputStream();
      var c = 0;
      while ((c = in.read()) != -1) {
        out.write(c);
      }
      soft.assertThat(out.toString(UTF_8)).isEqualTo(expected);
    }

    var buf = new byte[3];
    try (var in = new MultiByteArrayInputStream(bytes)) {
      var out = new ByteArrayOutputStream();
      var rd = 0;
      while ((rd = in.read(buf)) != -1) {
        out.write(buf, 0, rd);
      }
      soft.assertThat(out.toString(UTF_8)).isEqualTo(expected);
    }
  }

  static Stream<Arguments> reads() {
    return Stream.of(
        arguments(List.of("a".getBytes(UTF_8)), "a"),
        //
        arguments(List.of("a".getBytes(UTF_8), "b".getBytes(UTF_8), "c".getBytes(UTF_8)), "abc"),
        //
        arguments(
            List.of(
                new byte[0],
                "a2345678901234567890123456".getBytes(UTF_8),
                new byte[0],
                "b".getBytes(UTF_8),
                new byte[0],
                "c2345678901234567890123456".getBytes(UTF_8)),
            "a2345678901234567890123456bc2345678901234567890123456"),
        //
        arguments(
            List.of(
                new byte[0],
                ("a".repeat(123)).getBytes(UTF_8),
                ("b".repeat(77)).getBytes(UTF_8),
                ("c".repeat(13)).getBytes(UTF_8)),
            "a".repeat(123) + "b".repeat(77) + "c".repeat(13)),
        //
        arguments(List.of(), "")
        //
        );
  }
}

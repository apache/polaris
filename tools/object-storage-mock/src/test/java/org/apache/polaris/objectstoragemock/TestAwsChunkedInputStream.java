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
package org.apache.polaris.objectstoragemock;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.base.Strings;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.utils.IoUtils;

@ExtendWith(SoftAssertionsExtension.class)
@SuppressWarnings("InlineMeInliner")
public class TestAwsChunkedInputStream {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void chunked(byte[] input, int chunkSize) throws Exception {
    // See
    // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html#sigv4-chunked-body-definition
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    for (int p = 0; ; ) {
      int remain = input.length - p;
      int chunkLen = Math.min(remain, chunkSize);
      if (chunkLen > 0) {
        output.write(String.format("%x;chunk-signature=F00\r\n", chunkLen).getBytes(UTF_8));
        output.write(input, p, chunkLen);
        output.write("\r\n".getBytes(UTF_8));
        p += chunkLen;
      }
      if (p == input.length) {
        output.write("0;chunk-signature=BA5\r\n\r\n".getBytes(UTF_8));
        break;
      }
    }

    byte[] bytes = output.toByteArray();

    byte[] buffered;
    try (InputStream in = new AwsChunkedInputStream(new ByteArrayInputStream(bytes))) {
      buffered = IoUtils.toByteArray(in);
      soft.assertThat(in.read()).isEqualTo(-1);
      soft.assertThat(in.read()).isEqualTo(-1);
      soft.assertThat(in.read(new byte[10])).isEqualTo(-1);
      soft.assertThat(in.read(new byte[10])).isEqualTo(-1);
    }

    byte[] single = new byte[buffered.length];
    try (InputStream in = new AwsChunkedInputStream(new ByteArrayInputStream(bytes))) {
      for (int i = 0; i < buffered.length; i++) {
        int c = in.read();
        if (c == -1) {
          break;
        }
        single[i] = (byte) c;
      }
      soft.assertThat(in.read()).isEqualTo(-1);
      soft.assertThat(in.read()).isEqualTo(-1);
      soft.assertThat(in.read(new byte[10])).isEqualTo(-1);
      soft.assertThat(in.read(new byte[10])).isEqualTo(-1);
    }

    soft.assertThat(single).containsExactly(input);
    soft.assertThat(buffered).containsExactly(input);
  }

  static Stream<Arguments> chunked() {
    return Stream.of(
        arguments(new byte[0], 512),
        arguments(Strings.repeat("0123456789abcdef", 2000).getBytes(UTF_8), 512),
        arguments(Strings.repeat("0123456789abcdef", 2000).getBytes(UTF_8), 100),
        arguments(Strings.repeat("0123456789abcdef", 2000).getBytes(UTF_8), 65536));
  }

  @ParameterizedTest
  @MethodSource
  public void malformed(byte[] input, Class<? extends Exception> expected, String message) {
    soft.assertThatThrownBy(
            () -> IoUtils.toByteArray(new AwsChunkedInputStream(new ByteArrayInputStream(input))))
        .isInstanceOf(expected)
        .hasMessageStartingWith(message);
  }

  static Stream<Arguments> malformed() {
    return Stream.of(
        arguments(
            Strings.repeat("blahblah", 2000).getBytes(UTF_8),
            IllegalArgumentException.class,
            "metadata line too long"),
        arguments(
            "5;chunk-signature=F00\r".getBytes(UTF_8),
            EOFException.class,
            "Unexpected end of metadata line"),
        arguments(
            "5;chunk-signature=F00\nHello5;chunk-signature=F00\n".getBytes(UTF_8),
            EOFException.class,
            "Unexpected end of metadata line"),
        arguments(
            "5;chunk-signature=F00\r\nHello".getBytes(UTF_8),
            EOFException.class,
            "Expecting empty separator line, but got EOF"),
        arguments(
            "0;chunk-signature=F00\r\n".getBytes(UTF_8),
            EOFException.class,
            "Expecting empty separator line, but got EOF"),
        arguments(
            "5;chunk-signature=F00\r\nHell".getBytes(UTF_8),
            EOFException.class,
            "Unexpected end of chunk input, not enough data"),
        arguments(
            "5;chunk-signature=F00\r\n".getBytes(UTF_8),
            EOFException.class,
            "Unexpected end of chunk input, not enough data"),
        arguments(
            "5;chunk-signature=F00\rHello5;chunk-signature=F00\r".getBytes(UTF_8),
            IllegalArgumentException.class,
            "Illegal CR-LF sequence"),
        arguments(
            "FOO;chunk-signature=F00\r\nHello\r\n0;chunk-signature=F00\r\n\r\n".getBytes(UTF_8),
            NumberFormatException.class,
            ""));
  }
}

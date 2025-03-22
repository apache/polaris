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
package org.apache.polaris.apprunner.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.function.Consumer;

/** Captures input from an {@link InputStream} and emits full lines terminated with a {@code LF}. */
final class InputBuffer {

  private final Reader input;
  private final Consumer<String> output;
  private final StringBuilder lineBuffer = new StringBuilder();
  private boolean failed;

  InputBuffer(InputStream input, Consumer<String> output) {
    this(new BufferedReader(new InputStreamReader(input, Charset.defaultCharset())), output);
  }

  InputBuffer(Reader input, Consumer<String> output) {
    this.input = input;
    this.output = output;
  }

  /**
   * Drains the input passed to the constructor until there's no more data to read, captures full
   * lines terminated with a {@code LF}) and pushes these lines to the consumer passed into the
   * constructor.
   *
   * @return {@code true} if any data has been read from the input stream
   */
  boolean io() {
    // Note: cannot use BufferedReader.readLine() here, because that would block.
    try {
      if (failed || !input.ready()) {
        return false;
      }

      var any = false;
      while (input.ready()) {
        var c = input.read();

        if (c == -1) {
          return any;
        }

        any = true;
        switch (c) {
          case 13 -> { // CR
          }
          case 10 -> { // LF
            output.accept(lineBuffer.toString());
            lineBuffer.setLength(0);
          }
          default -> {
            lineBuffer.append((char) c);
          }
        }
      }
      return true;
    } catch (IOException e) {
      e.printStackTrace();
      failed = true;
      return false;
    }
  }

  void flush() {
    if (!lineBuffer.isEmpty()) {
      output.accept(lineBuffer.toString());
      lineBuffer.setLength(0);
    }
  }
}
